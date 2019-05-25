# Query Optimization

## What is Query Optimization
SQL Statement -> Query Optimization -> Awesome Query Plan


## Execution Flow
SQL Query -> Parser -> Query Optimization -> Query Execution Engine

Logical Operator: **what** they do
e.g. selection
  join
  union, project, grouping

Physical Operator: **how** they do
e.g. sequential scan, index scan
  nested loop join, sort-merge join, hash join, index join


## Query Optimization Steps
1. Enumerate logically equivalent plans by applying equivalent rules
2. For each logically equivalent plan, enumerate all alternative physical query plans
3. Estimate the cost of each of the alternative physical query plans
4. Run the plan with lowest estimated overall cost


## Equivalence Rules
Select and Join operators commute with each other
Join operator is associative
Select operator distributes over Joins
Project operator cascades


## Enumerate Alternative Physical Plans
for selection, we have sequential scan, and index scan
for join, we have nested loop join, sort-merge join, hash join, and index join


## Selectivity Estimation
Estimate how many rows will satisfy a predicate such as movie.id = 1
**Histogram** are the standard technique used to estimate **Selectivity factor** for predicates on a single table


