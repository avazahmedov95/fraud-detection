"""confirmed_cases: the accounts in confirmed frauds, read for the payee, the sender
and the cards the payee dealt with over the week."""

import config as C
import features as F
from conftest import payee_card
from rules import ReceiverState, SenderState


def _ev(sender="S1", payee="R1"):
    return {"amount_uzs": 150_000, "sender_pinfl": sender, "receiver_pinfl": payee,
            "sender_region": "Tashkent City",
            "sender_card": payee_card(sender), "receiver_card": payee_card(payee)}


def test_each_column_reads_the_confirmed_accounts():
    now = 1_000_000.0
    rs = ReceiverState(contacts={payee_card("A"): now - 100,
                                 payee_card("B"): now - C.LINK_WEEK_S - 1,   # too old
                                 payee_card("C"): now - 50})                 # not confirmed
    confirmed = {payee_card("R1"), payee_card("S1"), payee_card("A"), payee_card("B")}
    f = F.extract(_ev(), SenderState(), now, rs, confirmed=confirmed)
    assert (f["payee_flagged"], f["sender_flagged"], f["payee_flagged_contacts"]) == (1, 1, 1)
    clean = F.extract(_ev(), SenderState(), now, rs)
    assert (clean["payee_flagged"], clean["sender_flagged"],
            clean["payee_flagged_contacts"]) == (0, 0, 0)


def test_both_ends_of_a_transfer_file_the_other():
    payee, sender = ReceiverState(), ReceiverState()
    ev = _ev()
    F.update_receiver_state(payee, ev, 10.0)
    F.add_contact(sender, F.payee_key(ev), 10.0)
    assert payee.contacts == {payee_card("S1"): 10.0}
    assert sender.contacts == {payee_card("R1"): 10.0}
