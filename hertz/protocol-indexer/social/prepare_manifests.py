"""Keep the owning upstream processor workspace, pin its shared Kaspa SDK."""
from pathlib import Path
import re

root=Path('/src')
manifest=root/'Cargo.toml'
text=manifest.read_text()
text,count=re.subn(r'members = \[.*?\]', 'members = ["K-transaction-processor"]', text, count=1,flags=re.S)
assert count==1
manifest.write_text(text)
manifest=root/'K-transaction-processor/Cargo.toml'
text=manifest.read_text()
needle='kaspa-wallet-core = { git = "https://github.com/kaspanet/rusty-kaspa.git", features = ["wasm32-sdk"] }'
assert text.count(needle)==1
text=text.replace(needle,'kaspa-wallet-core = { git = "https://github.com/kaspanet/rusty-kaspa.git", rev = "01b532e8b553523216471682649693af92f0fd16", features = ["wasm32-sdk"] }')
text+='\n[features]\nshared-postgres-contract = []\n'
manifest.write_text(text)
