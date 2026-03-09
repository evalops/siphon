#!/usr/bin/env bash
set -euo pipefail

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

require_bin gh
require_bin jq

if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "GH_TOKEN is required" >&2
  exit 1
fi
if [[ -z "${REPO:-}" ]]; then
  echo "REPO is required (owner/repo)" >&2
  exit 1
fi
if [[ -z "${RUN_ID:-}" ]]; then
  echo "RUN_ID is required" >&2
  exit 1
fi

output_dir="${OUTPUT_DIR:-flake-metrics}"
workflow_file="${WORKFLOW_FILE:-ci.yml}"
branch="${BRANCH:-main}"
trend_limit="${TREND_RUN_LIMIT:-30}"

mkdir -p "${output_dir}"

jobs_file="$(mktemp)"
runs_file="$(mktemp)"
trap 'rm -f "${jobs_file}" "${runs_file}"' EXIT

# Current run snapshot for integration/perf-smoke status and timing.
gh api "repos/${REPO}/actions/runs/${RUN_ID}/jobs?per_page=100" >"${jobs_file}"

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg repo "${REPO}" \
  --argjson run_id "${RUN_ID}" \
  --arg run_url "https://github.com/${REPO}/actions/runs/${RUN_ID}" \
  --slurpfile jobs "${jobs_file}" \
  '{
    generated_at: $generated_at,
    repository: $repo,
    run_id: $run_id,
    run_url: $run_url,
    jobs: ($jobs[0].jobs
      | map(select(.name == "integration" or .name == "perf-smoke")
      | {
          name: .name,
          status: .status,
          conclusion: .conclusion,
          started_at: .started_at,
          completed_at: .completed_at,
          duration_seconds: (if (.started_at != null and .completed_at != null)
            then ((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601))
            else null
          end)
        }))
  }' >"${output_dir}/current.json"

printf 'run_id,run_number,run_created_at,run_conclusion,run_url,job_name,job_conclusion,job_started_at,job_completed_at,duration_seconds\n' >"${output_dir}/trend.csv"

# Trend window over recent CI runs on main.
gh api "repos/${REPO}/actions/workflows/${workflow_file}/runs?branch=${branch}&per_page=${trend_limit}" >"${runs_file}"

while IFS=$'\t' read -r run_id run_number run_created_at run_conclusion run_url; do
  run_jobs_file="$(mktemp)"
  gh api "repos/${REPO}/actions/runs/${run_id}/jobs?per_page=100" >"${run_jobs_file}"

  jq -r \
    --arg run_id "${run_id}" \
    --arg run_number "${run_number}" \
    --arg run_created_at "${run_created_at}" \
    --arg run_conclusion "${run_conclusion}" \
    --arg run_url "${run_url}" \
    '.jobs[]
      | select(.name == "integration" or .name == "perf-smoke")
      | [
          $run_id,
          $run_number,
          $run_created_at,
          $run_conclusion,
          $run_url,
          .name,
          (.conclusion // "unknown"),
          (.started_at // ""),
          (.completed_at // ""),
          (if (.started_at != null and .completed_at != null)
            then (((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601)) | tostring)
            else ""
          end)
        ]
      | @csv' "${run_jobs_file}" >>"${output_dir}/trend.csv"

  rm -f "${run_jobs_file}"
done < <(jq -r '.workflow_runs[] | [.id, .run_number, .created_at, .conclusion, .html_url] | @tsv' "${runs_file}")

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg repo "${REPO}" \
  --arg branch "${branch}" \
  --argjson run_id "${RUN_ID}" \
  --arg workflow_file "${workflow_file}" \
  --arg trend_csv "${output_dir}/trend.csv" \
  --arg current_json "${output_dir}/current.json" \
  '{
    generated_at: $generated_at,
    repository: $repo,
    branch: $branch,
    source_run_id: $run_id,
    workflow_file: $workflow_file,
    artifacts: {
      current: $current_json,
      trend: $trend_csv
    }
  }' >"${output_dir}/manifest.json"

echo "Flake metrics written to ${output_dir}"
