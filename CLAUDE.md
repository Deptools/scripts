# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This directory is used for writing small Python scripts for various purposes related to the DepTool project.

## Python Environment Setup

### Virtual Environment

A Python virtual environment is configured in this directory. To activate it:

```bash
# Activate the virtual environment
source venv/bin/activate

# Deactivate when done
deactivate
```

### Installing Dependencies

```bash
# Activate the virtual environment first
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt  # if requirements.txt exists

# Or install individual packages
pip install <package-name>
```

## Repository Structure

```
scripts/
├── venv/                              # Python virtual environment (not committed to git)
├── requirements.txt                   # Python dependencies
└── getRepoUrl/
    ├── githubRepo.csv                 # Maven artifacts to GitHub URLs mapping
    └── getRepoUrlWithLibrariesIo.py   # Script to fetch missing repository URLs
```

## Available Scripts

### getRepoUrlWithLibrariesIo.py

Automatically fetches GitHub repository URLs for Maven artifacts marked as "unknown" in the CSV file using the libraries.io API.

**Usage:**
```bash
# First time setup
source venv/bin/activate
pip install -r requirements.txt

# Run the script
cd getRepoUrl
python getRepoUrlWithLibrariesIo.py
```

**Features:**
- Reads `githubRepo.csv` and identifies artifacts with "unknown" repository URLs
- Queries the libraries.io API to fetch actual repository URLs
- Updates the CSV file progressively (saves after each successful update)
- Respects API rate limits (60 requests/minute)
- Handles interruptions gracefully - progress is saved
- Provides detailed logging of the process

**API Details:**
- Uses libraries.io API: `GET https://libraries.io/api/maven/:artifact?api_key=...`
- Rate limit: 60 requests per minute
- Returns repository URL in the `repository_url` field

## Data Files

### githubRepo.csv

A CSV file mapping Maven artifacts (groupId:artifactId format) to their corresponding GitHub repository URLs.

**Format:**
- Delimiter: semicolon (`;`)
- Column 1: `artifact` - Maven artifact identifier (e.g., "org.slf4j:slf4j-api")
- Column 2: `githubRepo` - GitHub repository URL or "unknown" if not available

This data is used by scripts to resolve source repositories for Java dependencies.