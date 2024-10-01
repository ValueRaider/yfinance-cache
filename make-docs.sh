#!/bin/bash


pip install sphinx


# Configure and fix the conf.py
mkdir docsrc
cd docsrc
sphinx-quickstart --no-sep -p yfinance-cache -a ValueRaider@protonmail.com -r 0.6.6 -l en
echo "   modules" >> index.rst
echo "" >> index.rst
sed -i "6i\
sys.path.insert(0, os.path.abspath('..'))" conf.py
sed -i "6i\
import sys" conf.py
sed -i "6i\
import os" conf.py
awk '/extensions = /{$0="extensions = ['\''sphinx.ext.autodoc'\'', '\''sphinx.ext.viewcode'\'', '\''sphinx.ext.napoleon'\'']"} 1' conf.py > temp && mv temp conf.py
awk '/html_theme =/{$0="html_theme = '\''sphinx_rtd_theme'\''"} 1' conf.py > temp && mv temp conf.py
cd ../


# Make html
sphinx-apidoc -o docsrc/ yfinance_cache
cd docsrc
make html
cd ../


# Copy HTML into a separate folder so Github Pages likes.
mkdir docs
rsync -av docsrc/_build/html/ docs/
rsync -av docsrc/modules.rst docs/
rsync -av docsrc/yfinance_cache.rst docs/
touch docs/.nojekyll
