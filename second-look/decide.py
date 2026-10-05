"""The second look's decision on one transfer. Pure, so it is tested without
TabPFN or Kafka."""

#: The model_version a second-look record carries, beside the served model's.
VERSION = "second-look"
#: Appended to it on a hold TabPFN did not score; the job writes VERSION + UNSCORED
#: itself when the second look is not answering (stream-processor/fraud_job.py).
UNSCORED = ":unscored"


def waited(event, now):
    """Seconds since the job decided to send the transfer here."""
    return now - float(event.get("scored_at_job") or now)


def verdict(event, score, cut, checkpoint):
    """The job's record of a transfer it sent for a second look, re-issued with the
    final decision: REVIEW holds it for the analyst, as a REVIEW from the job does,
    and ALLOW lets it go.

    `final_score` becomes TabPFN's - the score this decision was taken on - and
    `ml_score` stays the served model's, so an explanation is still checked against
    the model it explains. No score at all holds the transfer: the band is suspicious
    by construction, and letting it go unread would be the silent failure."""
    # The job's stage times describe the job's decision; copied, they would count twice.
    out = {k: v for k, v in event.items() if k != "stage_ms"}
    out["model_version"] = f"{VERSION}:{checkpoint}"
    if score is None:
        out["decision"] = "REVIEW"
        out["model_version"] += UNSCORED
    else:
        out["decision"] = "REVIEW" if score >= cut else "ALLOW"
        out["final_score"] = round(score, 4)
    return out
