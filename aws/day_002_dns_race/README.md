# A stale DNS plan: a Python simulation

This deterministic model illustrates the ordering problem described in the AWS post-event summary for October 19 and 20, 2025:
https://aws.amazon.com/message/101925/

A delayed writer passes a freshness check, a newer writer updates an endpoint, and the delayed writer overwrites it using its old permission. Cleanup then removes the active plan. The guarded path checks the generation when writing and excludes the active plan from cleanup.

## Run

Python 3.10 or newer; standard library only. From the repository root:

```sh
python aws/day_002_dns_race/dns_race_toy.py
python -m unittest discover -s aws/day_002_dns_race -p 'test_dns_race_toy.py' -v
```

Expected demo output:

```text
naive:   active = 1 | record = ()
naive:   next update fails: active plan 1 was deleted
guarded: late write accepted = False
guarded: active = 10 | record = ('10.0.10.1', '10.0.10.2')
```

## Tests and scope

The 12 unittest cases check the stale initial permission, deleted active plan, blocked subsequent update, rejected old/equal generation, successful newer generation, retained live plan and cleanup of unused older plans. The repository workflow runs this suite on Python 3.12. See the actual workflow results:
https://github.com/jenniferevangelista-de/data-engineering-lab/actions/workflows/tests.yml

The example uses synthetic addresses, one endpoint and one thread. It makes no network calls and requires no cloud credentials. It is not AWS code or an integration test with Route 53 or DynamoDB. It does not reproduce the downstream EC2/NLB cascade.

The guarded functions are not production concurrency primitives. Real storage must enforce version checks and reference protection atomically. The toy assumes plans were created by the planner before application; it does not validate missing plans, malformed generations, negative retention values or recovery from arbitrary corruption. A missing plan can still produce an empty record. Those cases are outside the demonstrated fixture.

## Files

- [Simulation](dns_race_toy.py)
- [Behavior tests](test_dns_race_toy.py)
