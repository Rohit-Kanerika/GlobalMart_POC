# GlobalMart — UC2: Returns Fraud Investigator

## Implementation Notes

### The 5 Scoring Rules and Their Weights

We designed a 100-point scoring system. A customer must score 50 or above to be flagged for investigation.

| Rule | Weight | Trigger |
| :--- | :--- | :--- |
| No matching purchase order | 40 | `has_orphan_return = 1` |
| High return volume | 20 | `total_returns > 3` |
| High total refund value | 20 | `total_return_value > $500` |
| Returns filed after policy window | 10 | `avg_days_to_return > 30` |
| Returns filed across multiple regions | 10 | `regions_returned_in > 1` |

**Why does the no-matching-order rule carry the most weight?**

At 40 points, triggering this rule alone nearly meets the investigation threshold. The reasoning is straightforward: if a customer is requesting a refund for an item that has no corresponding purchase record in `silver.orders`, one of two things happened — either the receipt is fraudulent, or the item was stolen. Either scenario is serious enough on its own to warrant much higher scrutiny than any of the behavioural signals. The remaining rules are additive indicators that reinforce suspicion, but none of them carry the same immediacy.

---

### The 50-Point Threshold Decision

The threshold was set by working through realistic combinations. Triggering the no-matching-order rule (40) plus any one secondary flag (10) reaches exactly 50. A customer with a clean purchase history who is simply abusing the return window would need High Volume (20) + High Value (20) + Multi-Region (10) to reach the threshold — three separate behavioural signals in combination.

If we lowered the threshold to 40, any single no-matching-order return would trigger an investigation. That would flood the returns team with cases where the explanation is simply a lost receipt or a POS sync lag — far too many false positives to be useful. A threshold that's too low defeats the purpose of priority-based investigation.

---

### How Many Customers Were Flagged?

In our dataset, approximately 9 customers crossed the 50-point threshold. For a returns team processing 40–60 cases daily, 9 high-priority flagged cases is a manageable and meaningful workload. It's focused enough that every investigation gets proper attention.

---

### Sample Investigation Brief (Highest-Scoring Customer)

**Customer: Aaron Smayling | Score: 70/100 | Rules Triggered: No Matching Order, High Volume, High Value, Multi-Region**

> "This case is highly suspicious because Customer 'Aaron Smayling' triggered an Anomaly Score of 70/100 by combining extreme aggregate behaviour with missing documentation. The data shows 6 total returns claiming over $850 in refunds, including at least one return with no traceable matching purchase in the order records. These returns were filed across multiple geographic regions and outside the standard 30-day return window.
>
> While there are benign explanations — a corporate buyer returning office supplies purchased on behalf of multiple offices, for example — the combination of cross-regional activity, volume, and the untraceable return closely mirrors organised retail fraud patterns.
>
> The returns team should prioritise verifying the physical inventory for the no-matching-order claim first. Any pending cash refunds should be converted to store credit pending identity verification, and the customer's purchase history should be reviewed across all regional systems before any further refunds are processed."

The brief accurately reflects all the triggered rules, names the specific data points (6 returns, $850), and gives the team a concrete first action.
