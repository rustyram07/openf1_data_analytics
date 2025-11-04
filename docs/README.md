# Documentation Index

**Comprehensive documentation for the F1 Data Warehouse on Databricks**

---

## Overview

This directory contains technical documentation for the OpenF1 Data Analytics platform - a production-grade data warehouse built on Databricks using the Medallion architecture.

---

## Documentation Structure

### 📐 Architecture & Design

#### [architecture.md](architecture.md)
**Complete system architecture documentation**

**Contents**:
- High-level architecture diagrams
- Technology stack details
- Data architecture (Medallion: Bronze → Silver → Gold)
- Storage architecture (Delta Lake, Unity Catalog)
- Processing architecture (dbt transformations)
- Security architecture (RBAC, access control)
- Deployment architecture (Dev/Stage/Prod)
- Design decisions & rationale

**When to Read**: Understanding system design, onboarding new team members, architectural reviews

---

#### [logical_data_model.md](logical_data_model.md)
**Detailed data model and schema documentation**

**Contents**:
- Entity Relationship Diagrams (ERD)
- Complete data dictionary (all 14 tables)
- Relationships & join paths
- Data types & constraints
- Slowly Changing Dimensions (SCD Type 2)
- Derived attributes & calculations
- Grain definitions

**When to Read**: Writing queries, understanding data relationships, building reports, data modeling reviews

---

### 🎯 Operations & Best Practices

#### [best_practices.md](best_practices.md)
**Comprehensive operations guide**

**Contents**:
1. **Orchestration**: Manual, GitHub Actions, Dagster patterns
2. **Data Ingestion**: API extraction, rate limiting, bronze loading
3. **Transformation & Storage**: dbt best practices, Delta Lake optimization
4. **Testing & Monitoring**: Testing layers, monitoring strategies
5. **Scaling & Performance**: Optimization techniques, capacity planning
6. **Governance & Security**: Unity Catalog, RBAC, data quality
7. **Operational Procedures**: Daily/weekly/monthly operations
8. **Idiosyncrasies & Gotchas**: Platform-specific issues and solutions

**When to Read**: Daily operations, troubleshooting, performance tuning, security configuration

---

## Quick Reference

### For Data Engineers

**Getting Started**:
1. Read: [architecture.md](architecture.md) - Sections 1-4
2. Read: [best_practices.md](best_practices.md) - Orchestration & Ingestion
3. Reference: [logical_data_model.md](logical_data_model.md) - As needed

**Daily Operations**:
- [best_practices.md](best_practices.md#operational-procedures) - Operations checklist
- [best_practices.md](best_practices.md#idiosyncrasies--gotchas) - Troubleshooting

### For Data Analysts

**Getting Started**:
1. Read: [logical_data_model.md](logical_data_model.md) - Gold Layer sections
2. Reference: [logical_data_model.md](logical_data_model.md#relationships--joins) - Query patterns

**Writing Queries**:
- [logical_data_model.md](logical_data_model.md#data-dictionary) - Column definitions
- [logical_data_model.md](logical_data_model.md#relationships--joins) - Join examples

### For DevOps

**CI/CD Setup**:
1. Read: [architecture.md](architecture.md#deployment-architecture)
2. Read: [../CI_CD_SETUP_GUIDE.md](../CI_CD_SETUP_GUIDE.md) - Step-by-step setup
3. Reference: [best_practices.md](best_practices.md#orchestration) - Automation patterns

### For Managers

**Architecture Review**:
- [architecture.md](architecture.md#executive-summary) - High-level overview
- [architecture.md](architecture.md#architecture-overview) - System diagrams
- [best_practices.md](best_practices.md#scaling--performance) - Capacity planning

---

## Document Map

```
docs/
├── README.md                    ← You are here
├── architecture.md              ← System architecture (35 pages)
│   ├── Executive Summary
│   ├── Architecture Overview
│   ├── Technology Stack
│   ├── Data Architecture
│   ├── Storage Architecture
│   ├── Processing Architecture
│   ├── Security Architecture
│   ├── Deployment Architecture
│   ├── Integration Points
│   └── Design Decisions
│
├── logical_data_model.md        ← Data model (40 pages)
│   ├── Overview
│   ├── Entity Relationship Diagrams
│   ├── Data Dictionary (14 tables)
│   ├── Relationships & Joins
│   ├── Data Types & Constraints
│   ├── Slowly Changing Dimensions
│   └── Derived Attributes
│
└── best_practices.md            ← Operations guide (50 pages)
    ├── Orchestration
    ├── Data Ingestion
    ├── Transformation & Storage
    ├── Testing & Monitoring
    ├── Scaling & Performance
    ├── Governance & Security
    ├── Operational Procedures
    └── Idiosyncrasies & Gotchas
```

---

## Related Documentation

### Project Root Documentation

| Document | Purpose |
|----------|---------|
| [../README.md](../README.md) | Project overview & quick start |
| [../QUICK_START.md](../QUICK_START.md) | 5-minute quick reference |
| [../CI_CD_SETUP_GUIDE.md](../CI_CD_SETUP_GUIDE.md) | CI/CD pipeline setup |
| [../CI_CD_TESTING_STEPS.md](../CI_CD_TESTING_STEPS.md) | Testing procedures |
| [../CI_CD_QUICK_TEST.md](../CI_CD_QUICK_TEST.md) | Quick test cheat sheet |
| [../GITHUB_SECRETS_REFERENCE.md](../GITHUB_SECRETS_REFERENCE.md) | GitHub secrets setup |

### dbt Documentation

| Document | Purpose |
|----------|---------|
| [../dbt_f1_data_analytics/SCHEMA_STRUCTURE.md](../dbt_f1_data_analytics/SCHEMA_STRUCTURE.md) | Schema docs with queries |
| [../dbt_f1_data_analytics/FINAL_BUILD_SUMMARY.md](../dbt_f1_data_analytics/FINAL_BUILD_SUMMARY.md) | Build status |
| [../dbt_f1_data_analytics/COMPLETE_VALIDATION.sql](../dbt_f1_data_analytics/COMPLETE_VALIDATION.sql) | Validation queries |
| [../dbt_f1_data_analytics/LAST_SESSION_VALIDATION.sql](../dbt_f1_data_analytics/LAST_SESSION_VALIDATION.sql) | Session validation |

---

## Documentation Standards

### Update Frequency

| Document | Update Trigger | Owner |
|----------|----------------|-------|
| **architecture.md** | Major architectural changes | Lead Engineer |
| **logical_data_model.md** | Schema changes, new tables | Data Engineer |
| **best_practices.md** | New patterns, gotchas discovered | Team |

### Version History

| Date | Document | Version | Changes |
|------|----------|---------|---------|
| 2025-11-04 | architecture.md | 1.0 | Initial creation |
| 2025-11-04 | logical_data_model.md | 1.0 | Initial creation |
| 2025-11-04 | best_practices.md | 1.0 | Initial creation |

### Contributing

When updating documentation:

1. **Follow the template**: Maintain consistent structure
2. **Add examples**: Include code snippets and SQL examples
3. **Update diagrams**: Keep visuals current
4. **Link references**: Cross-reference related sections
5. **Version bump**: Update version number and date
6. **Peer review**: Have another team member review changes

---

## Frequently Asked Questions

### Q: Where do I start?

**A**: Depends on your role:
- **New Engineer**: [architecture.md](architecture.md) → [best_practices.md](best_practices.md)
- **New Analyst**: [logical_data_model.md](logical_data_model.md) → Gold Layer sections
- **Manager**: [architecture.md](architecture.md#executive-summary)

### Q: How do I find the schema for a specific table?

**A**: [logical_data_model.md](logical_data_model.md#data-dictionary) - Use Ctrl+F to search for table name

### Q: How do I troubleshoot a pipeline failure?

**A**: [best_practices.md](best_practices.md#operational-procedures) - Incident Response section

### Q: How do I optimize a slow query?

**A**: [best_practices.md](best_practices.md#scaling--performance) - Performance Optimization section

### Q: How do I set up CI/CD?

**A**: [../CI_CD_SETUP_GUIDE.md](../CI_CD_SETUP_GUIDE.md) (project root, not in docs/)

### Q: What are the design decisions behind this architecture?

**A**: [architecture.md](architecture.md#design-decisions--rationale)

### Q: How do I handle Slowly Changing Dimensions?

**A**: [logical_data_model.md](logical_data_model.md#slowly-changing-dimensions)

### Q: What are common pitfalls to avoid?

**A**: [best_practices.md](best_practices.md#idiosyncrasies--gotchas)

---

## Feedback & Improvements

Found an error or want to improve the documentation?

1. **Create an Issue**: https://github.com/rustyram07/openf1_data_analytics/issues
2. **Submit a PR**: Update docs and create pull request
3. **Slack**: Post in #data-engineering channel

---

## Summary

| Document | Pages | Focus | Audience |
|----------|-------|-------|----------|
| **architecture.md** | 35 | System design | Engineers, Architects |
| **logical_data_model.md** | 40 | Data structures | Engineers, Analysts |
| **best_practices.md** | 50 | Operations | Engineers, DevOps |
| **Total** | **125** | Complete platform | All roles |

---

**Last Updated**: 2025-11-04
**Documentation Version**: 1.0
**Project**: OpenF1 Data Analytics on Databricks
