"""Link recorded model calls to the KAP agent job that made them.

KAP sends no job id to the model. A call belongs to a job when its first user
message equals the job's `user_prompt` and it happened while the job ran
(`running_at` … `finished_at`, with a little slack for clock skew). If two
jobs with the same prompt overlap in time, the calls cannot be split between
them and both jobs are marked ambiguous rather than guessed.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class JobWindow:
    id: str
    tenant_id: str
    user_prompt: str | None
    running_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class CallKey:
    id: str
    tenant_id: str | None
    started_at: datetime
    first_user_text: str | None


@dataclass
class Link:
    job_id: str
    status: str  # linked | ambiguous | none
    call_ids: list[str]


def _window(job: JobWindow, slack: timedelta) -> tuple[datetime, datetime] | None:
    if job.running_at is None or job.finished_at is None:
        return None
    return job.running_at - slack, job.finished_at + slack


def link_job(job: JobWindow, calls: list[CallKey], other_jobs: list[JobWindow], slack_s: float = 2.0) -> Link:
    """Decide which unlinked calls belong to `job`.

    `calls` are unlinked calls of the job's tenant near its window; `other_jobs`
    are other jobs of the tenant whose windows may overlap it.
    """
    slack = timedelta(seconds=slack_s)
    win = _window(job, slack)
    if win is None or not job.user_prompt:
        return Link(job.id, "none", [])
    lo, hi = win
    mine = [
        c
        for c in calls
        if c.first_user_text == job.user_prompt
        and lo <= c.started_at <= hi
        and (c.tenant_id is None or c.tenant_id == job.tenant_id)
    ]
    if not mine:
        return Link(job.id, "none", [])
    for other in other_jobs:
        if other.id == job.id or other.user_prompt != job.user_prompt:
            continue
        ow = _window(other, slack)
        if ow and ow[0] <= hi and lo <= ow[1]:
            return Link(job.id, "ambiguous", [])
    return Link(job.id, "linked", [c.id for c in sorted(mine, key=lambda c: c.started_at)])
