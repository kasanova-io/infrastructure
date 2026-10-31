"""Keep pinned K-webserver and all existing deployed compatibility patches."""
from pathlib import Path
import re,subprocess
root=Path('/src'); manifest=root/'Cargo.toml'
s,n=re.subn(r'members = \[.*?\]', 'members = ["K-webserver"]', manifest.read_text(),count=1,flags=re.S);assert n==1;manifest.write_text(s)
for name in ['historical-author-keys.patch','exact-vote-details.patch','content-search.patch','shared-api.patch','shared-read-api.patch']:
    subprocess.run(['patch','--batch','--fuzz=0','-p1','-i','/inputs/'+name],cwd=root,check=True)
