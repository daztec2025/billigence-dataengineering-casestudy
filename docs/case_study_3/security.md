# Case Study 3: Security & Compliance

## Email Protection Methods

### Display Masking
**Purpose:** Show data without revealing identity
**Implementation:** `*******@domain.com`
**Use:** UI displays, reports, analytics

### SHA-256 Hashing
**Purpose:** Secure storage and deduplication
**Implementation:** 256-bit cryptographic hash
**Use:** Database storage, verification, compliance

### Domain Preservation
**Purpose:** Enable analytics without PII
**Implementation:** Extract domain only (e.g., `gmail.com`)
**Use:** Marketing segmentation, provider analysis

## Access Control Architecture

### Production Dataset: `billigence_abc_customers_secure`
- Contains: Masked emails only
- Access: General analytics team
- Risk: Low

### Reference Table: `billigence_abc_email_reference`
- Contains: Original emails (PII)
- Access: Restricted to authorized users only
- Risk: High
- Requirement: Audit logging

## Compliance Alignment

### GDPR
**Pseudonymization (Article 4(5)):**
- Emails masked in production dataset
- Hash-based separation of identifying data

**Data Minimization (Article 5(1)(c)):**
- Only masked emails in analytics
- Original emails accessed only when necessary

### ISO 27001
**Information Security Controls:**
- SHA-256 encryption for data at rest
- Access controls and segregation
- Role-based access to sensitive data

**Risk Management:**
- Masked emails reduce exposure
- Hash cannot reveal original email

## Security Validation

```python
# No plain-text emails in production
plain_check = df.select("email").collect()
has_plain = any('@' in str(r['email']) and not str(r['email']).startswith('*')
                for r in plain_check)
assert not has_plain, "CRITICAL: Plain-text emails found!"

# All emails properly masked
invalid = df.filter(~F.col("email").rlike(r"^\*+@.+\..+$")).count()
assert invalid == 0, "Invalid masking"

# Hash length consistency (SHA-256 = 64 chars)
assert all(len(h) == 64 for h in df.select("email_sha256").collect())
```

## Best Practices

1. **Never log plain-text emails**
2. **Use parameterized queries** to prevent SQL injection
3. **Implement audit logging** for reference table access
4. **Apply data retention policies** (e.g., 90-day retention)
5. **Segregate datasets** by sensitivity level
