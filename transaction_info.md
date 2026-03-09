# Transaction Event

This project produces and consumes **Transaction** events as JSON messages.

Transactions are published to the Redis Stream `transactions`.

## Transaction JSON Schema

A transaction event contains the following fields:

- `transaction_id` (string): UUID identifying the transaction  
- `user_id` (string): Identifier for the user (e.g. `"u1"`)
- `amount` (number): Transaction amount (must be > 0)
- `currency` (string): Currency code such as `"GBP"` or `"EUR"`
- `merchant` (string): Merchant name (e.g. `"Amazon"`)
- `timestamp` (integer): UNIX epoch timestamp in seconds

## Example Event

```json
{
  "transaction_id": "8b2c1bd1-58b2-4b46-9f24-3f0d0b11b9f4",
  "user_id": "u1",
  "amount": 1999.99,
  "currency": "GBP",
  "merchant": "Tesco",
  "timestamp": 1710000123
}