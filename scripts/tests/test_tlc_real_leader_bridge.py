"""Bounded real-TLC leader witness versus compiled C++ election checkpoints.

The fixture is the archived ReachLeader.cfg.json from TLC 1.7.4 (2026-10-20),
not a hand-authored synthetic trace. This tests selected checkpoint relations,
not a full-state refinement proof: the model splits term/vote fsync and leader
promotion into actions that the C++ core combines.
"""

import base64
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest
import zlib

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
from test_tlc_witness_core_prefix import ProjectionMismatch, compare_prefix, require
from tlc_witness_prefix import canonical, message_bag, node_function, normalize

WITNESS_SHA256 = 'e5273d42f856bd8dbdc265bfc5cef6b0fcce0f18f10c091894a6a78b7ce4015c'
FIXTURE = HERE / 'fixtures' / 'reach_leader_tlc_20261020.zlib.b64'
RUNNER = None


def real_witness():
    compressed = base64.b64decode(FIXTURE.read_text(encoding='ascii').strip(), validate=True)
    raw = zlib.decompress(compressed)
    require(hashlib.sha256(raw).hexdigest() == WITNESS_SHA256, 'TLC witness digest mismatch')
    document = json.loads(raw)
    require(isinstance(document.get('config'), dict) and
            document['config'].get('name') == 'ReachLeader.cfg' and len(document['states']) == 9,
            'unexpected archived TLC witness')
    return document


def node(variables, field, actor='s2'):
    return node_function(variables[field])[actor]


def vote_name(value):
    require(isinstance(value, dict) and set(value) == {'$model'}, 'invalid model vote')
    return value['$model']


def check_model_split(witness):
    states = [s['variables'] for s in witness['states']]
    require(normalize({**witness, 'states': witness['states'][:3]}) ==
            ['timeout a', 'complete a'], 'real TLC initial prefix changed')
    require(len(states) == 9, 'expected nine model states')
    expected = [
        (0, 'Nil', 0, 'Nil'),
        (1, 'Nil', 0, 'Nil'),
        (1, 'Nil', 1, 'Nil'),
        (1, 's1', 1, 'Nil'),
        (1, 's1', 1, 's1'),
    ]
    for index, (term, voted, durable_term, durable_vote) in enumerate(expected, start=2):
        state = states[index]
        mem, disk = node(state, 'mem'), node(state, 'disk')
        require((mem['term'], vote_name(mem['votedFor']), disk['term'],
                 vote_name(disk['votedFor'])) == (term, voted, durable_term, durable_vote),
                f'actual TLC split term/vote mismatch at state {index + 1}')
    for index, target_vote in ((3, 'Nil'), (5, 's1')):
        pending = node(states[index], 'pending')
        require(isinstance(pending, dict) and pending['target']['term'] == 1 and
                vote_name(pending['target']['votedFor']) == target_vote,
                'TLC pending persistence target changed')
    for index in (4, 6):
        require(vote_name(node(states[index], 'pending')) == 'Nil',
                'TLC persistence callback did not clear pending')
    # Preserve the model's actual network-bag behavior across the split write.
    # UpdateTerm consumes s1's vote request to s2; neither fsync nor vote
    # staging is allowed to send a reply. Vote fsync adds exactly one reply,
    # and HandleRVResp consumes it. BecomeLeader does not emit model heartbeats.
    bags = [message_bag(state['net']) for state in states]
    request_bag = states[2]['net']
    require(isinstance(request_bag, dict) and set(request_bag) == {'$function'},
            'expected archived vote request bag')
    delivered = [msg for msg, count in request_bag['$function']
                 if count == 1 and isinstance(msg, dict) and
                 msg.get('mtype') == 'RVReq' and
                 msg.get('msrc') == {'$model': 's1'} and
                 msg.get('mdst') == {'$model': 's2'} and
                 msg.get('mterm') == 1]
    require(len(delivered) == 1, 'ambiguous delivered TLC vote request')
    after_delivery = dict(bags[2])
    key = canonical(delivered[0])
    require(after_delivery.get(key) == 1, 'missing delivered request in model bag')
    del after_delivery[key]
    require(bags[3] == after_delivery, 'UpdateTerm model delivery bag divergence')
    require(bags[4] == bags[3] and bags[5] == bags[4],
            'term fsync or vote staging changed model network')
    added = {k: n - bags[5].get(k, 0) for k, n in bags[6].items()
             if n > bags[5].get(k, 0)}
    require(len(added) == 1 and next(iter(added.values())) == 1,
            'vote fsync did not release exactly one model message')
    reply_key = next(iter(added))
    require(bags[6] == {**bags[5], reply_key: bags[5].get(reply_key, 0) + 1},
            'vote fsync changed unrelated network messages')
    reply_bag = states[6]['net']
    replies = [msg for msg, count in reply_bag['$function']
               if canonical(msg) == reply_key and count == 1]
    require(len(replies) == 1 and replies[0].get('mtype') == 'RVResp' and
            replies[0].get('msrc') == {'$model': 's2'} and
            replies[0].get('mdst') == {'$model': 's1'} and
            replies[0].get('mterm') == 1,
            'model durable vote reply routing/term divergence')
    require(bags[7] == bags[5] and bags[8] == bags[7],
            'vote reply delivery or leader promotion changed model bag')
    require(vote_name(node(states[7], 'role', 's1')) == 'Candidate' and
            vote_name(node(states[8], 'role', 's1')) == 'Leader',
            'TLC candidate-to-leader split changed')


def compare_bridge(witness, rows):
    check_model_split(witness)
    require(len(rows) == 5, 'wrong number of compiled core steps')
    compare_prefix({**witness, 'states': witness['states'][:3]}, rows[:2])
    for index, actor, action in ((2, 'b', 'HandleRVReq'),
                                 (3, 'b', 'PersistCompleted'),
                                 (4, 'a', 'HandleRVResp')):
        require(rows[index]['v'] == 2 and rows[index]['seq'] == index and
                rows[index]['node'] == actor and rows[index]['act'] == action,
                'unexpected compiled bridge transition identity')
        require(rows[index]['effects']['halt'] is False, 'core halted during bridge')
    staged, durable, elected = rows[2:]
    require((staged['post']['term'], staged['post']['votedFor']) == (1, 'a'),
            'combined core term/vote target diverged')
    require((staged['post']['durable']['term'], staged['post']['durable']['votedFor']) ==
            (0, None), 'core acknowledged term/vote before fsync')
    require(staged['pending_before'] is None and
            type(staged['pending_after']) is int and staged['pending_after'] > 0 and
            staged['effects']['persist'] == staged['pending_after'] and
            staged['effects']['send'] == [], 'core released vote before fsync')
    require((durable['post']['term'], durable['post']['votedFor'],
             durable['post']['durable']['term'], durable['post']['durable']['votedFor']) ==
            (1, 'a', 1, 'a'), 'durable vote-grant checkpoint divergence')
    require(durable['pending_before'] == staged['pending_after'] and
            durable['pending_after'] is None and durable['effects']['persist'] is None,
            'core durable vote callback/token divergence')
    replies = durable['effects']['send']
    require(len(replies) == 1 and replies[0]['kind'] == 'RVResp' and
            replies[0]['to'] == 'a' and replies[0]['term'] == 1 and
            replies[0]['positive'] is True and
            type(replies[0]['request_id']) is int and replies[0]['request_id'] > 0,
            'core vote reply destination/term/grant mismatch')
    requests = [msg for msg in rows[1]['effects']['send']
                if msg['kind'] == 'RVReq' and msg['to'] == 'b']
    require(len(requests) == 1 and
            replies[0]['request_id'] == requests[0]['request_id'],
            'core vote reply did not echo the delivered request identity')
    model = witness['states'][6]['variables']
    require((node(model, 'disk')['term'], vote_name(node(model, 'disk')['votedFor'])) ==
            (durable['post']['durable']['term'], 's1'),
            'model and core durable checkpoint differ')
    require(elected['post']['role'] == 'Leader' and
            elected['post']['term'] == 1 and
            elected['post']['durable']['term'] == 1 and
            elected['post']['durable']['votedFor'] == 'a',
            'core leader checkpoint divergence')
    heartbeats = elected['effects']['send']
    require(len(heartbeats) == 2 and
            {m['to'] for m in heartbeats} == {'b', 'c'} and
            all(m['kind'] == 'AEReq' and m['term'] == 1 and
                type(m['request_id']) is int and m['request_id'] > 0
                for m in heartbeats) and
            len({m['request_id'] for m in heartbeats}) == 2,
            'core leader heartbeats missing, duplicated, or misrouted')
    require(vote_name(node(witness['states'][8]['variables'], 'role', 's1')) ==
            elected['post']['role'], 'leader role checkpoint divergence')
    # Deliberately NOT comparing model states 4/5/6 or state 8 to a C++ row:
    # staged term/vote and leader promotion are split differently. The model
    # net bag also differs from the core's immediate heartbeats.


def run_core():
    commands = ['timeout a', 'complete a', 'deliver a b RVReq',
                'complete b', 'deliver b a RVResp']
    completed = subprocess.run([str(RUNNER), '--replay-inputs'],
                               input='\n'.join(commands) + '\n', text=True,
                               capture_output=True, timeout=20, check=False)
    require(completed.returncode == 0,
            'compiled runner rejected real-witness bridge schedule: ' + completed.stderr)
    try:
        return [json.loads(line) for line in completed.stdout.splitlines()]
    except json.JSONDecodeError as exc:
        raise ProjectionMismatch('compiled bridge output is invalid JSON') from exc


class RealTLCLeaderBridgeTests(unittest.TestCase):
    def test_archived_model_split_is_verified(self):
        check_model_split(real_witness())

    def test_model_negative_durable_vote_mutations(self):
        witness = real_witness()
        mutations = [
            lambda w: node(w['states'][4]['variables'], 'disk').update(votedFor={'$model': 's1'}),
            lambda w: node(w['states'][5]['variables'], 'disk').update(votedFor={'$model': 's1'}),
            lambda w: node(w['states'][6]['variables'], 'disk').update(votedFor={'$model': 'Nil'}),
            lambda w: node(w['states'][3]['variables'], 'pending')['target'].update(votedFor={'$model': 's1'}),
            lambda w: w['states'][3]['variables'].update(net=copy.deepcopy(w['states'][2]['variables']['net'])),
            lambda w: w['states'][4]['variables'].update(net={'$sequence': []}),
            lambda w: w['states'][5]['variables'].update(net={'$sequence': []}),
            lambda w: w['states'][6]['variables'].update(net=copy.deepcopy(w['states'][5]['variables']['net'])),
            lambda w: w['states'][7]['variables'].update(net=copy.deepcopy(w['states'][6]['variables']['net'])),
            lambda w: w['states'][8]['variables'].update(net={'$sequence': []}),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate), self.assertRaises(ProjectionMismatch):
                altered = copy.deepcopy(witness)
                mutate(altered)
                check_model_split(altered)

    def test_compiled_bridge_matches_selected_real_witness_checkpoints(self):
        if RUNNER is None:
            self.skipTest('compiled C++ runner not supplied')
        compare_bridge(real_witness(), run_core())

    def test_compiled_negative_mutations(self):
        if RUNNER is None:
            self.skipTest('compiled C++ runner not supplied')
        witness = real_witness()
        original = run_core()
        mutations = [
            lambda r: r[2]['post'].update(votedFor=None),
            lambda r: r[2]['post'].update(term=2),
            lambda r: r[2]['post']['durable'].update(term=1),
            lambda r: r[2]['post']['durable'].update(votedFor='a'),
            lambda r: r[2]['effects'].update(send=[{'kind': 'RVResp'}]),
            lambda r: r[3]['post']['durable'].update(votedFor=None),
            lambda r: r[3]['post']['durable'].update(term=0),
            lambda r: r[3].update(pending_before=999),
            lambda r: r[3]['effects'].update(send=[]),
            lambda r: r[3]['effects']['send'][0].update(to='c'),
            lambda r: r[3]['effects']['send'][0].update(positive=False),
            lambda r: r[3]['effects']['send'][0].update(request_id=999999),
            lambda r: r[4]['post'].update(role='Candidate'),
            lambda r: r[4]['effects'].update(send=[]),
            lambda r: r[4]['effects']['send'][0].update(to='a'),
            lambda r: r[4]['effects']['send'][1].update(request_id=r[4]['effects']['send'][0]['request_id']),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate), self.assertRaises(ProjectionMismatch):
                rows = copy.deepcopy(original)
                mutate(rows)
                compare_bridge(witness, rows)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        RUNNER = Path(sys.argv.pop(1)).resolve()
    unittest.main()