#!/usr/bin/env bash
# Synthetic rehearsal census allowances only. Sourced by cutover_rehearsal.sh;
# this does not enable an application feature or authorize a migration.
CENSUS_FEATURE_FLAGS=()
case "${CUTOVER_REHEARSAL_UPLOAD_INTENTS:-0}" in
  0) ;;
  1) CENSUS_FEATURE_FLAGS+=(--allow-empty-upload-intents) ;;
  *) echo 'invalid CUTOVER_REHEARSAL_UPLOAD_INTENTS (expected 0 or 1)' >&2; return 2 ;;
esac
case "${CUTOVER_REHEARSAL_INVENTORY:-0}" in
  0) ;;
  1) CENSUS_FEATURE_FLAGS+=(--allow-inventory-revision) ;;
  *) echo 'invalid CUTOVER_REHEARSAL_INVENTORY (expected 0 or 1)' >&2; return 2 ;;
esac
