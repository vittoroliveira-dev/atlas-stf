# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Repository

GitHub repository: `vittoroliveira-dev/atlas-stf`

Infer the repo from `git remote -v` when running inside this clone; `gh` does this automatically.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`
- **Create an issue with a multi-line body**: `gh issue create --title "..." --body-file -`
- **Read an issue**: `gh issue view <number> --comments`
- **Read an issue as JSON**: `gh issue view <number> --json number,title,body,labels,comments --jq '{number, title, body, labels: [.labels[].name], comments: [.comments[].body]}'`
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'`
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply a label**: `gh issue edit <number> --add-label "..."`
- **Remove a label**: `gh issue edit <number> --remove-label "..."`
- **Close an issue**: `gh issue close <number> --comment "..."`

## Skill behavior

When a skill says "publish to the issue tracker", create a GitHub issue.

When a skill says "fetch the relevant ticket", run `gh issue view <number> --comments`.
