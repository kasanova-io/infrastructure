FROM caddy@sha256:fce4f15aad23222c0ac78a1220adf63bae7b94355d5ea28eee53910624acedfa

# The upstream binary carries a low-port file capability. This site listens on
# 8080, so remove it to run with no capabilities and no-new-privileges enabled.
RUN setcap -r /usr/bin/caddy
USER 1000:1000
