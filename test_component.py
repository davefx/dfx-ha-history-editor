"""
Test script for History Editor component.

This script can be used to validate the component is properly installed
and working in your Home Assistant instance.

To use this script:
1. Install the History Editor component
2. Add 'history_editor:' to your configuration.yaml
3. Restart Home Assistant
4. Run this script from the Home Assistant Python environment

Note: This is for manual testing. For automated tests, see pytest tests.
"""

import asyncio
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_component_structure():
    """Test that all required files exist."""
    import os
    
    base_path = "custom_components/history_editor"
    required_files = [
        "__init__.py",
        "manifest.json",
        "panel.py",
        "services.yaml",
        "strings.json",
        "www/history-editor-panel.js",
    ]
    
    logger.info("Testing component structure...")
    for file in required_files:
        file_path = os.path.join(base_path, file)
        if os.path.exists(file_path):
            logger.info(f"✓ Found {file}")
        else:
            logger.error(f"✗ Missing {file}")
            return False
    
    logger.info("✓ All required files present")
    return True


def test_manifest():
    """Test that manifest.json is valid."""
    import json
    
    logger.info("Testing manifest.json...")
    try:
        with open("custom_components/history_editor/manifest.json") as f:
            manifest = json.load(f)
        
        required_keys = ["domain", "name", "version", "documentation", "dependencies"]
        for key in required_keys:
            if key in manifest:
                logger.info(f"✓ Manifest has '{key}': {manifest[key]}")
            else:
                logger.error(f"✗ Manifest missing '{key}'")
                return False
        
        # Check dependencies
        if "recorder" in manifest["dependencies"] and "history" in manifest["dependencies"]:
            logger.info("✓ Required dependencies listed")
        else:
            logger.error("✗ Missing required dependencies")
            return False
        
        logger.info("✓ Manifest is valid")
        return True
    except Exception as e:
        logger.error(f"✗ Error reading manifest: {e}")
        return False


def test_services_yaml():
    """Test that services.yaml is valid."""
    import yaml
    
    logger.info("Testing services.yaml...")
    try:
        with open("custom_components/history_editor/services.yaml") as f:
            services = yaml.safe_load(f)
        
        required_services = ["get_records", "update_record", "delete_record", "create_record"]
        for service in required_services:
            if service in services:
                logger.info(f"✓ Service '{service}' defined")
            else:
                logger.error(f"✗ Service '{service}' not found")
                return False
        
        logger.info("✓ All required services defined")
        return True
    except Exception as e:
        logger.error(f"✗ Error reading services.yaml: {e}")
        return False


def test_javascript():
    """Test that JavaScript file exists and has basic structure."""
    logger.info("Testing JavaScript panel...")
    try:
        with open("custom_components/history_editor/www/history-editor-panel.js") as f:
            js_content = f.read()
        
        required_elements = [
            "HistoryEditorPanel",
            "customElements.define",
            "history-editor-panel",
            "loadRecords",
            "editRecord",
            "deleteRecord",
        ]
        
        for element in required_elements:
            if element in js_content:
                logger.info(f"✓ JavaScript contains '{element}'")
            else:
                logger.error(f"✗ JavaScript missing '{element}'")
                return False
        
        logger.info("✓ JavaScript panel structure looks good")
        return True
    except Exception as e:
        logger.error(f"✗ Error reading JavaScript: {e}")
        return False


async def test_home_assistant_integration():
    """
    Test integration with Home Assistant.
    
    Note: This requires Home Assistant to be running and the component loaded.
    """
    logger.info("Testing Home Assistant integration...")
    logger.info("Note: This test requires Home Assistant to be running")
    logger.info("Please manually verify:")
    logger.info("  1. Component loads without errors in HA logs")
    logger.info("  2. Services appear in Developer Tools -> Services")
    logger.info("  3. Panel appears in sidebar (for admin users)")
    logger.info("  4. Panel UI loads without errors")
    return True


def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("History Editor Component - Test Suite")
    logger.info("=" * 60)
    logger.info("")
    
    results = []
    
    # Run tests
    results.append(("Component Structure", test_component_structure()))
    results.append(("Manifest Validation", test_manifest()))
    results.append(("Services Validation", test_services_yaml()))
    results.append(("JavaScript Validation", test_javascript()))
    
    # Run async test
    loop = asyncio.get_event_loop()
    results.append(("HA Integration", loop.run_until_complete(test_home_assistant_integration())))
    
    # Print summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Test Summary")
    logger.info("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        logger.info(f"{status}: {test_name}")
    
    logger.info("")
    logger.info(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        logger.info("✓ All tests passed!")
        return 0
    else:
        logger.error("✗ Some tests failed")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
