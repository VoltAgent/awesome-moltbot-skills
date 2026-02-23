#!/usr/bin/env python3
"""
Check the validity of links in awesome-openclaw-skills README.md.
Uses HEAD requests to check status codes, supports increasing GitHub API rate limits via GITHUB_TOKEN environment variable.
"""

import argparse
import os
import re
import ssl
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from http.client import HTTPResponse
from typing import Optional
from urllib.parse import urlparse


@dataclass
class LinkResult:
    """Link check result"""
    name: str
    url: str
    line_num: int
    original_line: str
    status_code: Optional[int]
    error: Optional[str]
    is_valid: bool


def extract_links_from_readme(filepath: str) -> list[tuple[str, str, int, str]]:
    """
    Extract all skill links from README.md.
    
    Returns: [(skill_name, url, line_num, original_line), ...]
    """
    pattern = re.compile(r'-\s+\[([^\]]+)\]\((https://github\.com/openclaw/skills/[^\)]+)\)')
    
    links = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            match = pattern.search(line)
            if match:
                name, url = match.groups()
                links.append((name, url, line_num, line.rstrip('\n')))
    
    return links


def check_link(name: str, url: str, github_token: Optional[str], timeout: int = 10) -> tuple[Optional[int], Optional[str], bool]:
    """
    Check a single link's validity using HEAD request.
    
    For GitHub links, use GITHUB_TOKEN for authentication to increase API limits.
    GitHub API limits:
    - Unauthenticated: 60 requests/hour
    - Authenticated: 5000 requests/hour
    
    Returns: (status_code, error_msg, is_valid)
    """
    # Build request
    parsed = urlparse(url)
    
    # Convert github.com links to API calls for more accurate status
    # e.g.: https://github.com/openclaw/skills/tree/main/skills/xxx/SKILL.md
    # -> https://api.github.com/repos/openclaw/skills/contents/skills/xxx/SKILL.md?ref=main
    
    is_github = parsed.netloc == 'github.com'
    
    if is_github:
        # Parse GitHub URL path
        path_parts = parsed.path.split('/')
        # /openclaw/skills/tree/main/skills/author/skill-name/SKILL.md
        if len(path_parts) >= 6 and path_parts[3] == 'tree':
            repo_owner = path_parts[1]
            repo_name = path_parts[2]
            branch = path_parts[4]
            file_path = '/'.join(path_parts[5:])
            
            # Build GitHub API URL
            api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{file_path}?ref={branch}"
            check_url = api_url
        else:
            check_url = url
    else:
        check_url = url
    
    # Create request
    req = urllib.request.Request(check_url, method='HEAD')
    
    # Set headers
    req.add_header('User-Agent', 'awesome-openclaw-skills-link-checker/1.0')
    
    if is_github and github_token:
        req.add_header('Authorization', f'token {github_token}')
        # GitHub API requires Accept header
        req.add_header('Accept', 'application/vnd.github.v3+json')
    
    # Create SSL context
    ssl_context = ssl.create_default_context()
    
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context) as response:
            # For GitHub API, HEAD requests may not be supported, handle it
            if isinstance(response, HTTPResponse):
                status_code = response.status
            else:
                status_code = 200
            
            is_valid = 200 <= status_code < 400
            
            return (status_code, None, is_valid)
    
    except urllib.error.HTTPError as e:
        status_code = e.code
        error_msg = None
        
        if status_code == 404:
            error_msg = "Not Found"
            is_valid = False
        elif status_code == 403:
            error_msg = "Forbidden (rate limited?)"
            # Rate limiting means the resource exists but is temporarily inaccessible
            is_valid = True
        elif status_code == 429:
            error_msg = "Too Many Requests"
            # Rate limiting means the resource exists but is temporarily inaccessible
            is_valid = True
        else:
            error_msg = f"HTTP {status_code}"
            is_valid = False
        
        return (status_code, error_msg, is_valid)
    
    except urllib.error.URLError as e:
        return (None, f"URL Error: {e.reason}", False)
    
    except TimeoutError:
        return (None, "Timeout", False)
    
    except Exception as e:
        return (None, f"Error: {str(e)}", False)


def check_all_links(
    links: list[tuple[str, str, int, str]],
    github_token: Optional[str],
    max_workers: int = 10,
    rate_limit_delay: float = 0.1
) -> list[LinkResult]:
    """
    Check all links concurrently.
    
    Args:
        links: [(name, url, line_num, original_line), ...]
        github_token: GitHub personal access token
        max_workers: Maximum number of concurrent workers
        rate_limit_delay: Delay between requests (in seconds)
    """
    results = []
    total = len(links)
    
    print(f"Checking {total} links...")
    print(f"Concurrency: {max_workers}")
    print(f"GITHUB_TOKEN: {'set' if github_token else 'not set (limit: 60/hour)'}")
    print("-" * 60)
    
    def check_with_delay(link_tuple):
        name, url, line_num, original_line = link_tuple
        status_code, error, is_valid = check_link(name, url, github_token)
        time.sleep(rate_limit_delay)  # Add delay to avoid triggering rate limits
        return LinkResult(
            name=name,
            url=url,
            line_num=line_num,
            original_line=original_line,
            status_code=status_code,
            error=error,
            is_valid=is_valid
        )
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(check_with_delay, link): link
            for link in links
        }
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            results.append(result)
            
            # Show progress (always print URL)
            status_icon = "✓" if result.is_valid else "✗"
            if result.is_valid:
                print(f"[{completed}/{total}] {status_icon} {result.name}")
                print(f"    {result.url}")
            else:
                error_info = result.error or f"HTTP {result.status_code}"
                print(f"[{completed}/{total}] {status_icon} {result.name} - {error_info}")
                print(f"    {result.url}")
    
    return results


def delete_invalid_lines(readme_path: str, results: list[LinkResult]) -> int:
    """
    Delete lines with invalid links from README.md.
    
    Returns: Number of deleted lines
    """
    # Collect line numbers to delete
    invalid_lines = {r.line_num for r in results if not r.is_valid}
    
    if not invalid_lines:
        return 0
    
    # Read all lines
    with open(readme_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Filter out invalid lines
    new_lines = [
        line for line_num, line in enumerate(lines, 1)
        if line_num not in invalid_lines
    ]
    
    # Write back to file
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    
    return len(invalid_lines)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Check the validity of links in README.md')
    parser.add_argument('--delete', action='store_true', help='Delete lines with invalid links')
    args = parser.parse_args()
    
    # Get README.md path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    readme_path = os.path.join(script_dir, "README.md")
    
    if not os.path.exists(readme_path):
        print(f"Error: Cannot find README.md file: {readme_path}")
        sys.exit(1)
    
    # Get GITHUB_TOKEN
    github_token = os.environ.get("GITHUB_TOKEN")
    
    # Extract links
    print(f"Reading {readme_path}...")
    links = extract_links_from_readme(readme_path)
    print(f"Found {len(links)} links")
    print()
    
    if not links:
        print("No links found")
        sys.exit(0)
    
    # Check links
    # For GitHub API, higher concurrency is possible with token
    # Without token, reduce concurrency to avoid triggering rate limits
    max_workers = 20 if github_token else 5
    rate_limit_delay = 0.05 if github_token else 0.5
    
    results = check_all_links(
        links,
        github_token,
        max_workers=max_workers,
        rate_limit_delay=rate_limit_delay
    )
    
    # Print summary
    print()
    print("=" * 60)
    valid_count = sum(1 for r in results if r.is_valid)
    invalid_count = len(results) - valid_count
    print(f"Check complete: {valid_count} valid, {invalid_count} invalid")
    
    # Delete invalid lines if requested
    if args.delete and invalid_count > 0:
        print()
        print("Deleting invalid links...")
        deleted = delete_invalid_lines(readme_path, results)
        print(f"Deleted {deleted} lines")
    
    # Return exit code
    if invalid_count > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()