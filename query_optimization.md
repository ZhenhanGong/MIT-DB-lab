# Query Optimization

## What is Query Optimization
SQL Statement -> Query Optimization -> Awesome Query Plan


## Execution Flow
SQL Query -> Parser -> Query Optimization -> Query Execution Engine

Logical Operator: **what** they do
e.g. union, selection, project, join, groping

Physical Operator: **how** they do
e.g. nested loop join, sort-merge, join, hash join, index join, filter


## Query Optimization Steps
- Enumerate logically equivalent plans by applying equivalent rules
- For each logically equivalent plan, enumerate all alternative physical query plans
- Estimate the cost of each of the alternative physical query plans
- Run the plan with lowest estimated overall cost


## Equivalence Rules
Select and Join operators commute with each other
Join operator is associative
Select operator distributes over Joins
Project operator cascades

