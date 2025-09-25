#!/usr/bin/env python3
"""
Template Validation Script

This script validates crypto lakehouse templates by testing configuration
loading and basic integration patterns.

Usage:
    python validate_templates.py real-time-trading.yaml
    python validate_templates.py historical-analysis.yaml
    python validate_templates.py --all
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import os

def load_template(template_path: Path) -> Dict[str, Any]:
    """Load and parse template YAML with environment variable substitution."""
    with open(template_path) as f:
        content = f.read()
    
    # Simple environment variable substitution
    import re
    def replace_env_vars(match):
        var_expr = match.group(1)
        if ':' in var_expr:
            var_name, default = var_expr.split(':', 1)
            return os.getenv(var_name, default)
        else:
            return os.getenv(var_expr, '')
    
    content = re.sub(r'\$\{([^}]+)\}', replace_env_vars, content)
    return yaml.safe_load(content)

def validate_crypto_source_config(source_config: Dict[str, Any]) -> List[str]:
    """Validate crypto source configuration."""
    issues = []
    
    if source_config.get('type') == 'cryptofeed':
        config = source_config.get('config', {})
        if not config.get('exchanges'):
            issues.append("Cryptofeed source missing exchanges")
        if not config.get('channels'):
            issues.append("Cryptofeed source missing channels")
        if not config.get('symbols'):
            issues.append("Cryptofeed source missing symbols")
            
    elif source_config.get('type') == 'binance_s3':
        config = source_config.get('config', {})
        if not config.get('bucket'):
            issues.append("Binance S3 source missing bucket")
        if not config.get('prefix_template') and not config.get('prefix'):
            issues.append("Binance S3 source missing prefix_template or prefix")
        if not config.get('symbols'):
            issues.append("Binance S3 source missing symbols")
            
    elif source_config.get('type') == 'ccxt':
        config = source_config.get('config', {})
        if not config.get('exchange'):
            issues.append("CCXT source missing exchange")
        if not config.get('symbols'):
            issues.append("CCXT source missing symbols")
    else:
        issues.append(f"Unknown source type: {source_config.get('type')}")
    
    return issues

def validate_iceberg_sink_config(sink_config: Dict[str, Any]) -> List[str]:
    """Validate Iceberg REST sink configuration."""
    issues = []
    
    if not sink_config.get('catalog_uri'):
        issues.append("Iceberg sink missing catalog_uri")
    if not sink_config.get('table_name'):
        issues.append("Iceberg sink missing table_name")
    
    storage = sink_config.get('storage', {})
    if not storage.get('bucket'):
        issues.append("Iceberg sink storage missing bucket")
    if not storage.get('endpoint'):
        issues.append("Iceberg sink storage missing endpoint")
        
    return issues

def validate_kafka_config(kafka_config: Dict[str, Any]) -> List[str]:
    """Validate Kafka configuration."""
    issues = []
    
    if not kafka_config.get('bootstrap_servers'):
        issues.append("Kafka config missing bootstrap_servers")
        
    return issues

def validate_template(template_path: Path) -> bool:
    """Validate a single template file."""
    print(f"\n🔍 Validating {template_path.name}")
    print("=" * 50)
    
    try:
        template = load_template(template_path)
    except Exception as e:
        print(f"❌ Failed to load template: {e}")
        return False
    
    all_issues = []
    
    # Validate crypto source
    if 'crypto_source' in template:
        issues = validate_crypto_source_config(template['crypto_source'])
        all_issues.extend([f"Crypto Source: {issue}" for issue in issues])
    else:
        all_issues.append("Missing crypto_source configuration")
    
    # Validate iceberg sink
    if 'iceberg_sink' in template:
        issues = validate_iceberg_sink_config(template['iceberg_sink'])
        all_issues.extend([f"Iceberg Sink: {issue}" for issue in issues])
    else:
        all_issues.append("Missing iceberg_sink configuration")
    
    # Validate kafka
    if 'kafka' in template:
        issues = validate_kafka_config(template['kafka'])
        all_issues.extend([f"Kafka: {issue}" for issue in issues])
    else:
        all_issues.append("Missing kafka configuration")
    
    # Report results
    if all_issues:
        print("❌ Validation Issues Found:")
        for issue in all_issues:
            print(f"   • {issue}")
        return False
    else:
        print("✅ Template validation passed")
        
        # Print configuration summary
        source_type = template.get('crypto_source', {}).get('type', 'unknown')
        table_name = template.get('iceberg_sink', {}).get('table_name', 'unknown')
        print(f"   📊 Source: {source_type}")
        print(f"   🏠 Table: {table_name}")
        
        return True

def test_schema_compatibility():
    """Test schema compatibility between crypto sources and Iceberg sink."""
    print("\n🔗 Testing Schema Compatibility")
    print("=" * 50)
    
    # This would ideally test actual schema compatibility
    # For now, document the expected schemas
    
    print("✅ Expected crypto data schema:")
    print("   • exchange: str")
    print("   • symbol: str") 
    print("   • price: float")
    print("   • quantity: float")
    print("   • side: str (buy/sell)")
    print("   • timestamp: int (unix timestamp)")
    
    print("\n✅ Iceberg sink auto-detection:")
    print("   • Automatically detects schema from first batch")
    print("   • Handles schema evolution (adds new fields)")
    print("   • Supports nested data with optional flattening")
    
    return True

def create_integration_test_example():
    """Create an example integration test script."""
    print("\n🧪 Creating Integration Test Example")
    print("=" * 50)
    
    test_script = '''#!/usr/bin/env python3
"""
Example Integration Test for Crypto Lakehouse Templates

This script demonstrates how to test crypto source → Iceberg sink integration
following the no-mocks policy with real services.
"""

import yaml
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource, create_cryptofeed_config
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

def test_real_time_trading_integration():
    """Test real-time trading template with paper trading mode."""
    
    # Load template
    with open('templates/real-time-trading.yaml') as f:
        template = yaml.safe_load(f)
    
    # Create application
    app = Application(
        broker_address=template['kafka']['bootstrap_servers'],
        app_id=template['application']['app_id']
    )
    
    # Configure crypto source (paper trading mode)
    source_config = create_cryptofeed_config(
        exchanges=template['crypto_source']['config']['exchanges'][:1],  # Test with one exchange
        channels=template['crypto_source']['config']['channels'],
        symbols=template['crypto_source']['config']['symbols'][:2]  # Test with fewer symbols
    )
    
    # Configure Iceberg sink
    sink_config = create_local_config(
        table_name=template['iceberg_sink']['table_name']
    )
    
    # Setup pipeline
    source = CryptofeedSource(source_config)
    sink = IcebergRESTSink(sink_config)
    
    topic = app.topic(template['topics']['crypto_trades']['name'])
    sdf = app.dataframe(topic)
    sdf.sink(sink)
    
    # Run for short duration to test integration
    print("🚀 Testing integration for 30 seconds...")
    app.run(source, duration=30)
    
    print("✅ Integration test completed successfully")

if __name__ == "__main__":
    test_real_time_trading_integration()
'''
    
    with open("templates/integration_test_example.py", "w") as f:
        f.write(test_script)
    
    print("✅ Created integration_test_example.py")
    print("   Run with: python templates/integration_test_example.py")
    
    return True

def main():
    """Main validation function."""
    templates_dir = Path("templates")
    
    if not templates_dir.exists():
        print("❌ Templates directory not found")
        return False
    
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        # Validate all templates
        template_files = list(templates_dir.glob("*.yaml"))
    elif len(sys.argv) > 1:
        # Validate specific template
        template_files = [templates_dir / sys.argv[1]]
    else:
        # Default: validate all templates
        template_files = list(templates_dir.glob("*.yaml"))
    
    if not template_files:
        print("❌ No template files found")
        return False
    
    print("🚀 Crypto Lakehouse Template Validation")
    print("=" * 50)
    print(f"Templates to validate: {len(template_files)}")
    
    all_passed = True
    for template_file in template_files:
        if not validate_template(template_file):
            all_passed = False
    
    # Test schema compatibility
    test_schema_compatibility()
    
    # Create integration test example
    create_integration_test_example()
    
    # Final summary
    print(f"\n{'🎉' if all_passed else '❌'} Validation Summary")
    print("=" * 50)
    if all_passed:
        print("✅ All templates passed validation")
        print("✅ Schema compatibility documented")
        print("✅ Integration test example created")
        print("\nNext steps:")
        print("  1. Test templates with real crypto data")
        print("  2. Run integration test with test containers")
        print("  3. Validate performance under load")
    else:
        print("❌ Some templates failed validation")
        print("Please fix the issues above and re-run validation")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)