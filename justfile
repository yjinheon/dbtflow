set positional-arguments

DBT := "uv run dbt"
DBT_DIRS := "--project-dir dvdrental --profiles-dir ."
WARN_ERROR := "--warn-error-options '{\"error\": [\"NoNodesForSelectionCriteria\"]}'"

_default:
  just --list

# setup
clean:
  {{ DBT }} clean {{ DBT_DIRS }}

deps:
  {{ DBT }} deps {{ DBT_DIRS }}

debug:
  {{ DBT }} debug {{ DBT_DIRS }}

parse:
  {{ DBT }} parse {{ DBT_DIRS }} --quiet {{ WARN_ERROR }}

# build / validation
build selector:
  {{ DBT }} build {{ DBT_DIRS }} --select {{ selector }} {{ WARN_ERROR }}

build-staging:
  {{ DBT }} build {{ DBT_DIRS }} --select path:models/01_staging {{ WARN_ERROR }}

build-intermediate:
  {{ DBT }} build {{ DBT_DIRS }} --select path:models/02_intermediate --quiet

build-marts:
  {{ DBT }} build {{ DBT_DIRS }} --select path:models/03_marts {{ WARN_ERROR }}

snapshot:
  {{ DBT }} snapshot {{ DBT_DIRS }} {{ WARN_ERROR }}

test selector:
  {{ DBT }} test {{ DBT_DIRS }} --select {{ selector }} {{ WARN_ERROR }}

# docs
docs-generate:
  {{ DBT }} docs generate {{ DBT_DIRS }}

docs-serve:
  {{ DBT }} docs serve {{ DBT_DIRS }}

# interactive helpers
select-build:
  {{ DBT }} build {{ DBT_DIRS }} --select $({{ DBT }} ls {{ DBT_DIRS }} --resource-type model | fzf) {{ WARN_ERROR }}

stg-select-build:
  {{ DBT }} build {{ DBT_DIRS }} --select $({{ DBT }} ls {{ DBT_DIRS }} --select path:models/01_staging --resource-type model | fzf) {{ WARN_ERROR }}

check-compile:
  {{ DBT }} compile {{ DBT_DIRS }} --select $({{ DBT }} ls {{ DBT_DIRS }} --resource-type model | fzf) --quiet {{ WARN_ERROR }}

show selector limit="10":
  {{ DBT }} show {{ DBT_DIRS }} --select {{ selector }} --limit {{ limit }}
