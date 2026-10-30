# Pinned source checkouts

These are independent Git repositories inside RelayHub, not Git submodules.
The parent repository ignores `third_party/*/` and tracks this document and
[`upstreams.lock.json`](upstreams.lock.json). The existing Docker context also
excludes these directories. All implementation source stays under
`~/workspace/projects/RelayHub/`; the homelab vault contains documentation only.

```text
RelayHub/third_party/
  README.md                 tracked by RelayHub
  upstreams.lock.json        tracked by RelayHub
  centrifugo/.git/           independent server repository
  centrifuge-js/.git/        independent browser/Node SDK repository
  centrifuge-go/.git/        independent Go SDK repository
  partykit/.git/             PartyServer/PartySocket/Yjs reference repository
```

## Initial baseline — 2026-10-31

| Repository | Upstream release | Commit |
|---|---|---|
| centrifugo | `v6.10.2` | `cf21c5de6be83a35e02b34e44ad40bdbc5556b9f` |
| centrifuge-js | `5.8.0` | `163dcbab9d5e23f41d1142e4631bcbabc2e2f62f` |
| centrifuge-go | `v0.13.0` | `b8ee54f4562141848b44b99125d2bfd69099e874` |
| partykit | `partyserver@0.5.10` | `53f0c6819bd534d80152c6a1b324cd17c9ab4e6d` |

The PartyKit snapshot contains PartyServer `0.5.10`, PartySocket `1.3.0`, and
y-partyserver `2.2.0`. A monorepo tag pins the whole tree; these package versions
do not mean that every package has an independently matching release tag.

Each checkout has full Git history, a local `relayhub` branch, an `upstream`
fetch remote, and `refs/relayhub/upstream-base` pointing to its pinned upstream
commit. The upstream push URL is deliberately disabled. No dependencies were
installed and no runtime compatibility is claimed by this source preparation.

`upstream_commit` identifies the upstream base we have integrated.
`checkout_commit` identifies the source revision RelayHub expects, including
our future changes. They are equal initially and will diverge when the fork
has its own commits. Tags are human-readable labels; the full commit hashes
are the authoritative pins.

## Restore a missing checkout

Read the repository's `path`, `upstream_url`, `upstream_commit` and
`checkout_commit` from the lock file. Run commands from the RelayHub root and
only clone into a missing directory. For the initial Centrifugo baseline:

```sh
git clone --origin upstream --no-checkout https://github.com/centrifugal/centrifugo.git third_party/centrifugo
git -C third_party/centrifugo switch -c relayhub cf21c5de6be83a35e02b34e44ad40bdbc5556b9f
git -C third_party/centrifugo update-ref refs/relayhub/upstream-base cf21c5de6be83a35e02b34e44ad40bdbc5556b9f
git -C third_party/centrifugo config remote.upstream.pushurl disabled://upstream-read-only
```

Use the other entries' URLs and hashes for their checkouts. Once fork commits
exist, add and fetch the recorded fork remote before checking out
`checkout_commit`; an upstream clone cannot restore unpublished fork commits.

## Compare upstream changes without modifying local code

Run inside the relevant nested repository:

```sh
git status --short
git fetch upstream --tags
git tag --list --sort=-version:refname
git diff --stat refs/relayhub/upstream-base HEAD
git log --oneline refs/relayhub/upstream-base..HEAD
```

Choose an actual release tag from the fetched tags. Replace `NEW_RELEASE` below
with that tag; these commands only inspect it:

```sh
git rev-parse 'refs/tags/NEW_RELEASE^{commit}'
git log --oneline refs/relayhub/upstream-base..refs/tags/NEW_RELEASE
git diff --stat refs/relayhub/upstream-base refs/tags/NEW_RELEASE
git diff refs/relayhub/upstream-base refs/tags/NEW_RELEASE
```

For PartyKit, focus on `packages/partyserver`, `packages/partysocket`,
`packages/y-partyserver`, shared utilities and their tests. Read release notes,
dependency changes and protocol changes in addition to the diff. Fetching
does not update the local `relayhub` branch. Do not automatically pull, reset,
or overwrite the fork when a new release appears.

## Integrate an upstream release

1. Commit existing work in the nested repo. Create a candidate upgrade branch
   from `relayhub`, and merge the selected upstream commit there. Preserve
   upstream history; review conflict resolutions against both implementations.
2. Run upstream tests and fork tests. For server/SDK upgrades, run the pinned
   original/fork client-by-server matrix, real browser/Node/Go checks, and
   RelayHub auth, recovery and durable-agent regression checks.
3. Review the candidate and move the nested `relayhub` branch to the verified
   result. Keep the previous pinned fork commit available for rollback.
4. Push the nested repo's own commits to its GitHub and GitLab fork remotes.
   Record those remotes in the lock file. Only after the source is retrievable
   should the parent lock point at a fork-only commit.
5. Update `upstream_tag`, `upstream_commit`, `checkout_commit`, relevant package
   versions, verification status and date in the parent lock. Update the local
   `refs/relayhub/upstream-base` to that accepted upstream commit. Commit and push
   the lock/documentation change separately in RelayHub and reconcile homelab
   notes.

The reference PartyKit repository can advance without implying that its changes
were ported. Record any implemented upstream features against their source
commit separately. Selectively copying a patch does not make the entire new
upstream release the integrated base.

## Ownership of changes

`git status` and commits in RelayHub intentionally do not include source changes
inside these nested repositories. Run Git inside the nested repository to
review and commit its changes. Never use `git add -f third_party/<repo>` in the
parent and never add node_modules/build outputs to either repository.

The initial lock has empty `fork_remotes`: only upstream source baselines have
been cloned; no hosted user forks or modified dependency releases exist yet.
Before the first custom-source release, create/configure the owned GitHub and
GitLab remotes, push the nested commits there and record the exact URLs and
commit pins. A parent-repository push alone does not back up nested changes.
