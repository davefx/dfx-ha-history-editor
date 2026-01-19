# Contributing to History Editor

First off, thank you for considering contributing to History Editor! It's people like you that make this component better for everyone.

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. Please be respectful and constructive in all interactions.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When you create a bug report, include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples**
- **Describe the behavior you observed and what you expected**
- **Include screenshots if relevant**
- **Include your Home Assistant version and component version**
- **Include relevant logs from Home Assistant**

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion:

- **Use a clear and descriptive title**
- **Provide a detailed description of the suggested enhancement**
- **Explain why this enhancement would be useful**
- **List any similar features in other integrations**

### Pull Requests

- Fork the repository and create your branch from `main`
- If you've added code, add tests if applicable
- Ensure your code follows the existing style
- Update the README.md if you change functionality
- Write a clear commit message

## Development Setup

### Prerequisites

- Python 3.11 or higher
- Home Assistant development environment
- Basic knowledge of Home Assistant architecture

### Setting Up Development Environment

1. Clone the repository:
```bash
git clone https://github.com/davefx/dfx-ha-history-editor.git
cd dfx-ha-history-editor
```

2. Create a test Home Assistant configuration:
```bash
mkdir -p config
cp configuration.yaml.example config/configuration.yaml
```

3. Install Home Assistant in development mode (in a virtual environment):
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install homeassistant
```

4. Link the component to your test installation:
```bash
ln -s $(pwd)/custom_components/history_editor ~/.homeassistant/custom_components/history_editor
```

### Testing

Run the validation script:
```bash
python test_component.py
```

Test the component in Home Assistant:
```bash
hass -c config
```

### Code Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) for Python code
- Use type hints where appropriate
- Add docstrings to functions and classes
- Keep functions focused and small
- Use meaningful variable names

### JavaScript Style

- Use modern ES6+ features
- Keep the UI responsive and accessible
- Follow existing naming conventions
- Comment complex logic

## Project Structure

```
dfx-ha-history-editor/
├── custom_components/
│   └── history_editor/
│       ├── __init__.py          # Main component logic and services
│       ├── manifest.json         # Component metadata
│       ├── panel.py             # Frontend panel registration
│       ├── services.yaml        # Service definitions
│       ├── strings.json         # Localization strings
│       └── www/
│           └── history-editor-panel.js  # Frontend UI
├── examples/                    # Usage examples
├── test_component.py           # Validation tests
├── README.md                   # User documentation
└── CONTRIBUTING.md            # This file
```

## Key Areas for Contribution

### Backend

- **Database operations**: Improve query performance, add pagination
- **Error handling**: Better error messages and validation
- **New services**: Additional CRUD operations or bulk actions
- **Security**: Input validation, permission checks

### Frontend

- **UI improvements**: Better table design, filtering, sorting
- **UX enhancements**: Loading states, better error messages
- **New features**: Export/import functionality, charts
- **Accessibility**: Keyboard navigation, screen reader support

### Documentation

- **README improvements**: Clearer instructions, more examples
- **Code comments**: Explain complex logic
- **Tutorials**: Step-by-step guides for common use cases
- **Translations**: Support for multiple languages

### Testing

- **Unit tests**: Test individual functions
- **Integration tests**: Test with actual Home Assistant instance
- **Edge cases**: Handle unusual scenarios gracefully

## Questions?

Feel free to open an issue with your question or reach out to the maintainers.

## License

By contributing, you agree that your contributions will be licensed under the same Apache License 2.0 that covers the project.
