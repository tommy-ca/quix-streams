"""
Fast Test Execution Utilities

Provides helper functions for timeout-aware, efficient test execution
during TDD development cycles.

Author: TDD Sprint 3 - REFACTOR Phase
Date: September 19, 2025
"""

import time
import subprocess
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import signal


class TimeoutError(Exception):
    """Raised when a test operation exceeds timeout."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for timeout management."""
    raise TimeoutError("Operation timed out")


@contextmanager
def timeout_context(seconds: int):
    """Context manager for timeout operations."""
    # Set up timeout signal
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        # Clean up
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def fast_pytest_run(test_path: str, timeout_seconds: int = 30, 
                   extra_args: List[str] = None) -> Dict[str, Any]:
    """
    Execute pytest with timeout and return structured results.
    
    Args:
        test_path: Path to test file or directory
        timeout_seconds: Maximum execution time
        extra_args: Additional pytest arguments
        
    Returns:
        Dict with test results and metadata
    """
    extra_args = extra_args or []
    cmd = [
        sys.executable, "-m", "pytest", 
        test_path,
        "--tb=no",  # No traceback for speed
        "-q",       # Quiet output
        "--timeout=60"  # Per-test timeout
    ] + extra_args
    
    start_time = time.time()
    try:
        with timeout_context(timeout_seconds):
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=timeout_seconds
            )
            
        execution_time = time.time() - start_time
        
        # Parse results
        success = result.returncode == 0
        output_lines = result.stdout.split('\n') if result.stdout else []
        
        # Extract test counts
        summary_line = [line for line in output_lines if "passed" in line or "failed" in line]
        test_count = len([line for line in output_lines if "::" in line])
        
        return {
            "success": success,
            "execution_time": execution_time,
            "test_count": test_count,
            "output": result.stdout,
            "errors": result.stderr,
            "summary": summary_line[0] if summary_line else "No summary",
            "timeout_exceeded": False
        }
        
    except (subprocess.TimeoutExpired, TimeoutError):
        return {
            "success": False,
            "execution_time": timeout_seconds,
            "test_count": 0,
            "output": "TIMEOUT",
            "errors": f"Test execution exceeded {timeout_seconds}s timeout",
            "summary": "TIMEOUT",
            "timeout_exceeded": True
        }
    except Exception as e:
        return {
            "success": False,
            "execution_time": time.time() - start_time,
            "test_count": 0,
            "output": "",
            "errors": str(e),
            "summary": f"ERROR: {e}",
            "timeout_exceeded": False
        }


def fast_coverage_check(module_path: str, timeout_seconds: int = 15) -> Dict[str, Any]:
    """
    Quick coverage check with timeout.
    
    Args:
        module_path: Python module path for coverage
        timeout_seconds: Maximum execution time
        
    Returns:
        Dict with coverage results
    """
    cmd = [
        sys.executable, "-m", "pytest",
        "--cov=" + module_path,
        "--cov-report=term-missing",
        "--cov-report=json",
        "-q",
        "--tb=no"
    ]
    
    start_time = time.time()
    try:
        with timeout_context(timeout_seconds):
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds
            )
        
        execution_time = time.time() - start_time
        
        # Look for coverage percentage in output
        coverage_line = None
        for line in result.stdout.split('\n'):
            if 'TOTAL' in line and '%' in line:
                coverage_line = line
                break
                
        # Extract percentage
        coverage_percent = 0
        if coverage_line:
            try:
                parts = coverage_line.split()
                for part in parts:
                    if '%' in part:
                        coverage_percent = int(part.replace('%', ''))
                        break
            except:
                pass
        
        return {
            "success": result.returncode == 0,
            "execution_time": execution_time,
            "coverage_percent": coverage_percent,
            "coverage_line": coverage_line,
            "output": result.stdout,
            "timeout_exceeded": False
        }
        
    except (subprocess.TimeoutExpired, TimeoutError):
        return {
            "success": False,
            "execution_time": timeout_seconds,
            "coverage_percent": 0,
            "coverage_line": "TIMEOUT",
            "output": "Coverage check timed out",
            "timeout_exceeded": True
        }


def validate_test_framework() -> Dict[str, Any]:
    """
    Quick validation of testing framework setup.
    
    Returns:
        Dict with validation results
    """
    project_root = Path(__file__).parents[2]
    results = {
        "pytest_config": False,
        "coverage_config": False,
        "integration_fixtures": False,
        "benchmark_baseline": False,
        "ci_workflow": False,
        "overall_status": "FAILED"
    }
    
    try:
        # Check pytest config
        pyproject = project_root / "pyproject.toml"
        if pyproject.exists():
            content = pyproject.read_text()
            if "[tool.pytest.ini_options]" in content and "iceberg_rest" in content:
                results["pytest_config"] = True
        
        # Check coverage config  
        if "[tool.coverage.run]" in content and "quixstreams/sinks/community/iceberg_rest" in content:
            results["coverage_config"] = True
            
        # Check integration fixtures
        fixtures_file = project_root / "tests" / "integration" / "fixtures.py"
        if fixtures_file.exists():
            results["integration_fixtures"] = True
            
        # Check benchmark baseline
        baseline_file = project_root / ".benchmarks" / "iceberg_rest_baseline.json"
        if baseline_file.exists():
            results["benchmark_baseline"] = True
            
        # Check CI workflow
        workflow_file = project_root / ".github" / "workflows" / "iceberg_rest_tests.yml"
        if workflow_file.exists():
            results["ci_workflow"] = True
            
        # Overall status
        passed = sum(1 for v in results.values() if isinstance(v, bool) and v)
        total = len([k for k in results.keys() if isinstance(results[k], bool)])
        
        if passed == total:
            results["overall_status"] = "PASSED"
        elif passed >= total * 0.8:  # 80%
            results["overall_status"] = "MOSTLY_PASSED"
        else:
            results["overall_status"] = "FAILED"
            
        results["score"] = f"{passed}/{total}"
        
    except Exception as e:
        results["error"] = str(e)
        
    return results


def quick_smoke_test() -> Dict[str, Any]:
    """
    Execute a quick smoke test of the entire framework.
    
    Returns:
        Dict with smoke test results
    """
    project_root = Path(__file__).parents[2]
    
    start_time = time.time()
    results = {
        "framework_validation": None,
        "config_helpers_test": None,
        "local_stack_test": None,
        "integration_fixtures_test": None,
        "overall_success": False,
        "execution_time": 0
    }
    
    try:
        # 1. Framework validation (5s)
        results["framework_validation"] = validate_test_framework()
        
        # 2. Config helpers test (10s)
        config_test_path = str(project_root / "tests" / "test_config_helpers.py")
        if Path(config_test_path).exists():
            results["config_helpers_test"] = fast_pytest_run(config_test_path, 10)
            
        # 3. Local stack test (10s)
        stack_test_path = str(project_root / "tests" / "test_local_stack.py")
        if Path(stack_test_path).exists():
            results["local_stack_test"] = fast_pytest_run(stack_test_path, 10)
            
        # 4. Integration fixtures test (5s)
        fixtures_path = str(project_root / "tests" / "integration" / "fixtures.py")
        if Path(fixtures_path).exists():
            # Just try to import it
            try:
                import importlib.util
                spec = importlib.util.spec_from_file_location("fixtures", fixtures_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                results["integration_fixtures_test"] = {"success": True, "execution_time": 0.1}
            except Exception as e:
                results["integration_fixtures_test"] = {"success": False, "error": str(e)}
                
        # Overall success
        success_count = 0
        total_count = 0
        
        for key, value in results.items():
            if isinstance(value, dict) and "success" in value:
                total_count += 1
                if value["success"]:
                    success_count += 1
                    
        results["overall_success"] = success_count == total_count and success_count > 0
        results["execution_time"] = time.time() - start_time
        results["success_rate"] = f"{success_count}/{total_count}"
        
    except Exception as e:
        results["error"] = str(e)
        results["execution_time"] = time.time() - start_time
        
    return results


if __name__ == "__main__":
    # Quick self-test
    print("🧪 Testing Framework Utilities - Self Test")
    
    # Test framework validation
    framework_result = validate_test_framework()
    print(f"Framework Validation: {framework_result['overall_status']} ({framework_result['score']})")
    
    # Test smoke test
    smoke_result = quick_smoke_test()
    print(f"Smoke Test: {'PASSED' if smoke_result['overall_success'] else 'FAILED'} "
          f"({smoke_result['success_rate']}) in {smoke_result['execution_time']:.1f}s")
    
    print("✅ Test utilities ready for fast TDD execution!")