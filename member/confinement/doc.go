// Package confinement proves on a real ClickHouse that the stage-3 member
// users are confined (housegate spec 2026-10-14 §8.2 as amended, §17; Plan
// S3-C2 contract §23.2-§23.3): on a server with
// access_control_improvements.table_engines_require_grant, si_member creates
// no replicated, Buffer, Distributed, Merge or Alias table and no replicated
// database, moves no data into hg_member_unsafe and changes no table there,
// writes nothing through remote() or to S3, overrides no named collection,
// drops no replica, and holds no dictionary, SYSTEM or definer right and
// nothing on the protocol databases, while every right its role needs works;
// si_member_ddl's profile refuses a replica created with merges allowed, and
// it can neither write, alter nor optimize. The package holds tests only:
// they are opt-in with ARBITER_CH_MEMBER_CONFINEMENT=1 and fail, rather than
// skip, when a prerequisite is missing.
// scripts/ci/clickhouse-member-confinement.sh starts the servers and runs
// them (README § Member role (stage 3 P2)).
package confinement
