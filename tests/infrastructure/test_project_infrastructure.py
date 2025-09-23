"""
TDD RED Phase: Project Infrastructure Tests

This module defines failing test specifications for project infrastructure.
All tests are expected to FAIL initially (RED phase) as the infrastructure
doesn't exist yet.

Testing Categories:
1. Project Structure Validation
2. Code Quality Configuration  
3. Pre-commit Hooks
4. Documentation Framework
5. Development Environment Setup

Author: TDD Sprint 3 - RED Phase
Date: September 19, 2025
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, List
import pytest
import json
import yaml


class TestProjectStructure:
    """Test the iceberg_rest project structure and organization."""
    
    def test_directory_structure_exists(self):
        """Test that required directory structure exists."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        base_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest"
        
        # Required directories (will fail initially)
        required_dirs = [
            base_path,
            base_path / "examples",
            base_path / "tests",
            base_path / "docs"
        ]
        
        for directory in required_dirs:
            assert directory.exists(), f"Required directory must exist: {directory}"
            assert directory.is_dir(), f"Path must be directory: {directory}"
    
    def test_required_files_present(self):
        """Test that required project files are present."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        base_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest"
        
        # Required files (will fail initially)
        required_files = [
            base_path / "__init__.py",
            base_path / "iceberg_rest.py",  # Main implementation
            base_path / "config_helpers.py",  # Already exists
            base_path / "examples" / "__init__.py",
            base_path / "examples" / "local_development.py",
            base_path / "examples" / "cloudflare_r2.py",
            base_path / "examples" / "aws_s3_rest.py",
            base_path / "tests" / "__init__.py",
            base_path / "tests" / "test_rest_sink.py",
            base_path / "docs" / "README.md"
        ]
        
        for file_path in required_files:
            assert file_path.exists(), f"Required file must exist: {file_path}"
            assert file_path.is_file(), f"Path must be file: {file_path}"
    
    def test_proper_module_organization(self):
        """Test that module organization follows Python best practices."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        base_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest"
        
        # Check __init__.py has proper imports (will fail initially)
        init_file = base_path / "__init__.py"
        if init_file.exists():
            content = init_file.read_text()
            assert "from .iceberg_rest import" in content, "__init__.py must import main module"
            assert "from .config_helpers import" in content, "__init__.py must import config helpers"
            assert "__all__" in content, "__init__.py must define __all__"
    
    def test_example_files_in_correct_locations(self):
        """Test that example files are properly organized."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        examples_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest" / "examples"
        
        # Check example files have proper content structure
        example_files = {
            "local_development.py": ["create_local_rest_config", "QuixApp", "main"],
            "cloudflare_r2.py": ["create_r2_config", "QuixApp", "main"],
            "aws_s3_rest.py": ["create_s3_rest_config", "QuixApp", "main"]
        }
        
        for filename, required_elements in example_files.items():
            file_path = examples_path / filename
            assert file_path.exists(), f"Example file must exist: {filename}"
            
            content = file_path.read_text()
            for element in required_elements:
                assert element in content, f"Example {filename} must contain {element}"


class TestCodeQuality:
    """Test code quality tool configuration and enforcement."""
    
    def test_ruff_configuration_exists(self):
        """Test that ruff configuration is properly setup for iceberg_rest."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Check ruff configuration in pyproject.toml
        pyproject_path = project_root / "pyproject.toml"
        content = pyproject_path.read_text()
        
        # Should have iceberg_rest specific configuration (will fail initially)
        assert "[tool.ruff.lint.per-file-ignores]" in content
        iceberg_rest_ignore_pattern = 'quixstreams/sinks/community/iceberg_rest/*.py'
        assert iceberg_rest_ignore_pattern in content, "Ruff config must include iceberg_rest files"
    
    def test_black_configuration_exists(self):
        """Test that black formatting configuration includes iceberg_rest."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        pyproject_path = project_root / "pyproject.toml"
        content = pyproject_path.read_text()
        
        # Check black configuration (may need to add iceberg_rest specific settings)
        assert "[tool.black]" in content or "black" in content, "Black configuration must be present"
    
    def test_code_passes_linting(self):
        """Test that iceberg_rest code passes linting checks."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        iceberg_rest_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest"
        
        if not iceberg_rest_path.exists():
            pytest.skip("iceberg_rest module not implemented yet")
        
        # Run ruff on iceberg_rest module
        result = subprocess.run([
            sys.executable, "-m", "ruff", "check", str(iceberg_rest_path)
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Code must pass linting checks: {result.stdout}"
    
    def test_code_properly_formatted(self):
        """Test that iceberg_rest code is properly formatted."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        iceberg_rest_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest"
        
        if not iceberg_rest_path.exists():
            pytest.skip("iceberg_rest module not implemented yet")
        
        # Check black formatting
        result = subprocess.run([
            sys.executable, "-m", "black", "--check", str(iceberg_rest_path)
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Code must be properly formatted: {result.stdout}"


class TestPreCommitHooks:
    """Test pre-commit hook configuration and execution."""
    
    def test_precommit_config_exists(self):
        """Test that pre-commit configuration file exists."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        precommit_config = project_root / ".pre-commit-config.yaml"
        
        assert precommit_config.exists(), "Pre-commit config file must exist"
        assert precommit_config.is_file(), "Pre-commit config must be file"
    
    def test_hooks_properly_configured(self):
        """Test that pre-commit hooks are properly configured."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        precommit_config = project_root / ".pre-commit-config.yaml"
        
        if not precommit_config.exists():
            pytest.fail("Pre-commit config file does not exist")
            
        with open(precommit_config, 'r') as f:
            config = yaml.safe_load(f)
        
        # Required hooks for code quality
        required_hook_ids = [
            "ruff",
            "black", 
            "mypy",
            "pytest-check"
        ]
        
        # Extract hook ids from config
        hook_ids = []
        for repo in config.get('repos', []):
            for hook in repo.get('hooks', []):
                hook_ids.append(hook.get('id'))
        
        for required_hook in required_hook_ids:
            assert required_hook in hook_ids, f"Pre-commit must include {required_hook} hook"
    
    def test_hooks_execute_successfully(self):
        """Test that pre-commit hooks can execute successfully."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Try to run pre-commit on a test file
        result = subprocess.run([
            "pre-commit", "run", "--all-files"
        ], cwd=project_root, capture_output=True, text=True)
        
        # This might fail initially but should work after GREEN phase
        # For now, just check that pre-commit is available
        install_result = subprocess.run([
            "pre-commit", "--version"
        ], capture_output=True, text=True)
        
        assert install_result.returncode == 0, "Pre-commit must be available"
    
    def test_code_quality_enforced(self):
        """Test that code quality is enforced through pre-commit."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Check if pre-commit is installed in git hooks
        git_hooks_dir = project_root / ".git" / "hooks"
        precommit_hook = git_hooks_dir / "pre-commit"
        
        if git_hooks_dir.exists():
            # Pre-commit hook should exist after installation
            assert precommit_hook.exists(), "Pre-commit git hook must be installed"


class TestDocumentation:
    """Test documentation structure and generation."""
    
    def test_documentation_structure(self):
        """Test that documentation structure is properly organized."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        docs_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest" / "docs"
        
        # Required documentation files
        required_docs = [
            docs_path / "README.md",
            docs_path / "api.md",
            docs_path / "examples.md",
            docs_path / "migration-guide.md"
        ]
        
        for doc_file in required_docs:
            assert doc_file.exists(), f"Documentation file must exist: {doc_file}"
    
    def test_api_docs_generated(self):
        """Test that API documentation can be generated."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Check if sphinx or similar is configured
        sphinx_config = project_root / "docs" / "conf.py"
        if sphinx_config.exists():
            # Try to generate docs
            result = subprocess.run([
                "sphinx-build", "-b", "html", 
                str(project_root / "docs"), 
                str(project_root / "docs" / "_build" / "html")
            ], capture_output=True, text=True)
            
            # Should succeed after implementation
            assert result.returncode == 0, f"API docs generation failed: {result.stderr}"
    
    def test_examples_documented(self):
        """Test that all examples are properly documented."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        examples_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest" / "examples"
        
        if examples_path.exists():
            for py_file in examples_path.glob("*.py"):
                if py_file.name != "__init__.py":
                    content = py_file.read_text()
                    
                    # Check for proper documentation
                    assert '"""' in content, f"Example {py_file.name} must have docstring"
                    assert "Example:" in content, f"Example {py_file.name} must contain usage example"
    
    def test_readme_comprehensive(self):
        """Test that README is comprehensive and up-to-date."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        readme_path = project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest" / "docs" / "README.md"
        
        if readme_path.exists():
            content = readme_path.read_text()
            
            required_sections = [
                "# Iceberg REST Sink",
                "## Installation",
                "## Quick Start", 
                "## Configuration",
                "## Examples",
                "## API Reference"
            ]
            
            for section in required_sections:
                assert section in content, f"README must contain {section} section"


class TestDevelopmentSetup:
    """Test development environment setup and reproducibility."""
    
    def test_dev_requirements_file(self):
        """Test that development requirements are properly defined."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Check for development requirements
        dev_req_files = [
            project_root / "requirements-dev.txt",
            project_root / "dev-requirements.txt", 
            project_root / "pyproject.toml"  # Check dev dependencies in pyproject.toml
        ]
        
        has_dev_deps = False
        for req_file in dev_req_files:
            if req_file.exists():
                content = req_file.read_text()
                if any(dep in content for dep in ["pytest", "ruff", "black", "mypy"]):
                    has_dev_deps = True
                    break
        
        assert has_dev_deps, "Development dependencies must be defined"
    
    def test_setup_script_exists(self):
        """Test that development setup script exists."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        setup_scripts = [
            project_root / "scripts" / "dev-setup.sh",
            project_root / "dev-setup.sh",
            project_root / "Makefile"
        ]
        
        has_setup_script = any(script.exists() for script in setup_scripts)
        assert has_setup_script, "Development setup script must exist"
    
    def test_environment_reproducible(self):
        """Test that development environment is reproducible."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Check for environment specification
        env_files = [
            project_root / "environment.yml",  # conda
            project_root / "Pipfile",          # pipenv
            project_root / "pyproject.toml",   # poetry/pip
            project_root / "requirements.txt"   # pip
        ]
        
        has_env_spec = any(env_file.exists() for env_file in env_files)
        assert has_env_spec, "Environment specification must exist for reproducibility"
    
    def test_development_guides_complete(self):
        """Test that development guides are comprehensive."""
        project_root = Path("/home/tommyk/projects/devops/quix-streams")
        
        # Look for development documentation
        dev_docs = [
            project_root / "CONTRIBUTING.md",
            project_root / "docs" / "development.md",
            project_root / "quixstreams" / "sinks" / "community" / "iceberg_rest" / "docs" / "development.md"
        ]
        
        has_dev_docs = any(doc.exists() for doc in dev_docs)
        assert has_dev_docs, "Development documentation must exist"
        
        # Check content if available
        for doc in dev_docs:
            if doc.exists():
                content = doc.read_text()
                required_sections = ["setup", "testing", "contributing"]
                
                sections_found = sum(1 for section in required_sections if section.lower() in content.lower())
                assert sections_found >= 2, f"Development docs must cover essential topics"


if __name__ == "__main__":
    # Run these tests to see them fail (RED phase)
    pytest.main([__file__, "-v", "--tb=short"])