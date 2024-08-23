import os
import sys
sys.path.insert(0, os.path.abspath('..'))

project = 'yfinance-cache'
copyright = '2024, Your Name'
author = 'Your Name'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
]

html_theme = 'sphinx_rtd_theme'

# Add this to focus on yfc_ticker.py
autodoc_mock_imports = ['yfinance_cache']
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
}