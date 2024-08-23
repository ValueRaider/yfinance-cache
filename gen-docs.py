import os
import shutil
import subprocess

# Generate documentation
subprocess.run(["pdoc", "--html", "--output-dir", "docs", "yfinance_cache"])

# Move all HTML files to docs root
for filename in os.listdir("docs/yfinance_cache"):
    if filename.endswith(".html"):
        shutil.move(f"docs/yfinance_cache/{filename}", f"docs/{filename}")

# Rename yfc_ticker.html to index.html
os.rename("docs/yfc_ticker.html", "docs/index.html")

# Update links in all HTML files
for filename in os.listdir("docs"):
    if filename.endswith(".html"):
        with open(f"docs/{filename}", "r") as f:
            content = f.read()
        
        content = content.replace('href="index.html"', 'href="yfinance_cache.html"')
        content = content.replace('href="yfc_', 'href="yfc_')
        
        with open(f"docs/{filename}", "w") as f:
            f.write(content)

# Remove the now-empty yfinance_cache folder
shutil.rmtree("docs/yfinance_cache")

print("Documentation generated successfully.")