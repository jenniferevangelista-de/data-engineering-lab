"""A toy model of the race AWS described for DynamoDB's DNS automation.

Not AWS code. One endpoint, numbered plan generations, two Enactors,
one cleanup job, all in a single thread so every step is explicit.
"""
from dataclasses import dataclass, field


class InconsistentState(Exception):
    pass


@dataclass
class Endpoint:
    plans: dict[int, tuple[str, ...]] = field(default_factory=dict)
    active: int | None = None

    def record(self) -> tuple[str, ...]:
        """What a client resolves: the IPs of the active plan, if it still exists."""
        return self.plans.get(self.active, ())


def planner(ep: Endpoint, generations) -> None:
    for gen in generations:
        ep.plans[gen] = (f"10.0.{gen}.1", f"10.0.{gen}.2")


# Naive version: check once at the start, write later, clean up by age.
def check_newer(ep: Endpoint, gen: int) -> bool:
    return ep.active is None or gen > ep.active


def write_plan(ep: Endpoint, gen: int) -> None:
    if ep.active is not None and ep.active not in ep.plans:
        raise InconsistentState(f"active plan {ep.active} was deleted")
    ep.active = gen


def cleanup(ep: Endpoint, newest: int, keep: int = 2) -> None:
    for gen in [g for g in ep.plans if g < newest - keep]:
        del ep.plans[gen]


# Guarded version: check at write time, never delete what is live.
def write_if_newer(ep: Endpoint, gen: int) -> bool:
    if ep.active is not None and gen <= ep.active:
        return False
    ep.active = gen
    return True


def safe_cleanup(ep: Endpoint, newest: int, keep: int = 2) -> None:
    for gen in [g for g in ep.plans if g < newest - keep and g != ep.active]:
        del ep.plans[gen]


def naive_run() -> Endpoint:
    ep = Endpoint()
    planner(ep, range(1, 11))
    slow_ok = check_newer(ep, 1)   # Enactor A checks generation 1, then stalls
    write_plan(ep, 10)             # Enactor B applies generation 10
    if slow_ok:
        write_plan(ep, 1)          # A wakes up, its old check still says yes
    cleanup(ep, newest=10)         # B's cleanup deletes "old" plans, 1 included
    return ep


def guarded_run() -> tuple[Endpoint, bool]:
    ep = Endpoint()
    planner(ep, range(1, 11))
    write_if_newer(ep, 10)
    late = write_if_newer(ep, 1)   # A's late write is checked now, and refused
    safe_cleanup(ep, newest=10)
    return ep, late


if __name__ == "__main__":
    ep = naive_run()
    print("naive:   active =", ep.active, "| record =", ep.record())
    try:
        write_plan(ep, 11)
    except InconsistentState as e:
        print("naive:   next update fails:", e)
    ep, late = guarded_run()
    print("guarded: late write accepted =", late)
    print("guarded: active =", ep.active, "| record =", ep.record())
