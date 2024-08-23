import os
import shutil
import subprocess

# Generate documentation
subprocess.run(["pdoc", "--html", "--output-dir", "docs", "yfinance_cache"])

# Move files to docs root, but don't overwrite index.html
for filename in os.listdir("docs/yfinance_cache"):
    # if filename.endswith(".html") and filename != "index.html":
    if filename in ['yfc_ticker.html', 'index.html']:
        shutil.move(f"docs/yfinance_cache/{filename}", f"docs/{filename}")

# # Update the content of index.html to load yfc_ticker.html
# with open("docs/yfinance_cache/index.html", "r") as f:
#     index_content = f.read()

# index_content = index_content.replace(
#     '<a class="homelink" rel="home" title="yfinance_cache Home" href="./index.html">',
#     '<a class="homelink" rel="home" title="yfinance_cache Home" href="./yfc_ticker.html">'
# )

# with open("docs/index.html", "w") as f:
#     f.write(index_content)

# Remove the yfinance_cache folder
shutil.rmtree("docs/yfinance_cache")

print("Documentation generated successfully.")