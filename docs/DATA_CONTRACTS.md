> Scope: consult [README](../README.md) and [REVIEW](REVIEW.md) for tested local behavior. Cloud/distributed mappings in these learning notes are illustrative and unvalidated.

# Data Contracts
The root `data_contract.json` defines required fields and allowed order channels. The pipeline fails before warehouse loading when the contract is violated. Numeric positivity and duplicate order IDs are additional semantic checks.
