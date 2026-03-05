# API Contract — Transaction Event

This project produces and consumes **Transaction** events as JSON messages.

## Transaction JSON Schema

A transaction event has the following fields:

- `transaction_id` (string): UUID (e.g. `"8b2c1bd1-58b2-4b46-9f24-3f0d0b11b9f4"`)
- `user_id` (string): Identifier for the user (e.g. `"u123"`)
- `amount` (number): Transaction amount (must be > 0)
- `currency` (string): 3-letter ISO 4217 currency code (e.g. `"GBP"`, `"EUR"`)
- `merchant` (string): Merchant name (e.g. `"Amazon"`)
- `timestamp` (integer): UNIX epoch seconds (e.g. `1710000123`)

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