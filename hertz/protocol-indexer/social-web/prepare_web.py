"""Keep pinned K-webserver and all existing deployed compatibility patches."""
from pathlib import Path
import argparse,re,subprocess
parser=argparse.ArgumentParser()
parser.add_argument('--source', type=Path, default=Path('/src'))
parser.add_argument('--inputs', type=Path, default=Path('/inputs'))
args=parser.parse_args()
root=args.source; manifest=root/'Cargo.toml'
s,n=re.subn(r'members = \[.*?\]', 'members = ["K-webserver"]', manifest.read_text(),count=1,flags=re.S);assert n==1;manifest.write_text(s)
for name in ['historical-author-keys.patch','exact-vote-details.patch','content-search.patch','shared-api.patch','shared-read-api.patch','tip-counts.patch','protocol-metadata.patch','unified-feed.patch','edited-metadata.patch','poll-results.patch','alias-links.patch','translations.patch']:
    subprocess.run(['patch','--batch','--fuzz=0','-p1','-i',str(args.inputs/name)],cwd=root,check=True)
