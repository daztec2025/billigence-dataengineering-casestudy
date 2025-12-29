#!/bin/bash
set -e

CASE_STUDY="${CASE_STUDY:-all}"

echo "Starting LocalSpark Analysis - Case Study: $CASE_STUDY"

case "$CASE_STUDY" in
    1)
        echo "Running Case Study 1: Retail Electronics Analysis"
        python scripts/case_study_1_analysis.py
        ;;
    2)
        echo "Running Case Study 2: University API Analysis"
        python scripts/case_study_2_analysis.py
        ;;
    3)
        echo "Running Case Study 3: Customer Data Security"
        python scripts/case_study_3_analysis.py
        ;;
    all)
        echo "Running all case studies"
        python scripts/case_study_1_analysis.py
        python scripts/case_study_2_analysis.py
        python scripts/case_study_3_analysis.py
        ;;
    *)
        echo "Invalid CASE_STUDY value: $CASE_STUDY"
        echo "Valid options: 1, 2, 3, all"
        exit 1
        ;;
esac

echo "Analysis complete!"
