# Contributing to OrientDB enteprise agent

You want to contribute to OrientDB enteprise agent? Welcome! Please read this document to understand what you can do:
 * [Help Others](#help-others)
 * [Analyze Issues](#analyze-issues)
 * [Report an Issue](#report-an-issue)
 * [Contribute Code](#contribute-code)

## Help Others

You can help OrientDB enterprise agent by helping others who use OrientDB and need support. You will find them e.g. on [StackOverflow](http://stackoverflow.com/questions/tagged/orientdb)

## Analyze Issues

Analyzing issue reports can be a lot of effort. Any help is welcome!
Go to [the Github issue tracker](https://github.com/SAP/orientdb-enterprise-agent/issues?state=open) and find an open issue which needs additional work or a bugfix.

Additional work may be further information, or a minimized jsbin example or gist, or it might be a hint that helps understanding the issue. Maybe you can even find and [contribute](#contribute-code) a bugfix?


## Report an Issue

If you find a bug - behavior of OrientDB enterprise agent code contradicting its specification - you are welcome to report it.
We can only handle well-reported, actual bugs, so please follow the guidelines below and use forums like [StackOverflow](http://stackoverflow.com/questions/tagged/orientdb) for support questions or when in doubt whether the issue is an actual bug.

Once you have familiarized with the guidelines, you can go to the [Github issue tracker for OrientDB enterprise agent](https://github.com/SAP/orientdb-enterprise-agent/issues/new) to report the issue.

### Quick Checklist for Bug Reports

Issue report checklist:
 * Real, current bug
 * No duplicate
 * Reproducible
 * Good summary
 * Well-documented
 * Minimal example



### Issue handling process

When an issue is reported, a committer will look at it and either confirm it as a real issue (by giving the "in progress" label), close it if it is not an issue, or ask for more details. In-progress issues are then either assigned to a committer in GitHub, reported in our internal issue handling system, or left open as "contribution welcome" for easy or not urgent fixes.

An issue that is about a real bug is closed as soon as the fix is committed. The closing comment explains which patch version(s) of OrientDB enteprise agent will contain the fix.


### Reporting Security Issues

We take security issues in our projects seriously. We appreciate your efforts to responsibly disclose your findings.

Please do not report security issues directly on GitHub but using one of the channels listed below. This allows us to provide a fix before an issue can be exploited.

- **Researchers/Non-SAP Customers:** Please consult SAPs [disclosure guidelines](https://wiki.scn.sap.com/wiki/display/PSR/Disclosure+Guidelines+for+SAP+Security+Advisories) and send the related information in a PGP encrypted e-mail to secure@sap.com. Find the public PGP key [here](https://www.sap.com/dmc/policies/pgp/keyblock.txt).
- **SAP Customers:** If the security issue is not covered by a published security note, please report it by creating a customer message at https://launchpad.support.sap.com.

Please also refer to the general [SAP security information page](https://www.sap.com/about/trust-center/security/incident-management.html).

### Usage of Labels

Github offers labels to categorize issues. We defined the following labels so far:

Labels for issue categories:
 * bug: this issue is a bug in the code
 * documentation: this issue is about wrong documentation
 * enhancement: this is not a bug report, but an enhancement request

Status of open issues:
 * unconfirmed: this report needs confirmation whether it is really a bug (no label; this is the default status)
 * in progress: this issue has been triaged and is now being handled, e.g. because it looks like an actual bug
 * author action: the author is required to provide information
 * contribution welcome: this fix/enhancement is something we would like to have and you are invited to contribute it

Status/resolution of closed issues:
 * fixed: a fix for the issue was provided
 * duplicate: the issue is also reported in a different ticket and is handled there
 * invalid: for some reason or another this issue report will not be handled further (maybe lack of information or issue does not apply anymore)
 * works: not reproducible or working as expected
 * wontfix: while acknowledged to be an issue, a fix cannot or will not be provided

The labels can only be set and modified by committers.


### Issue Reporting Disclaimer

We want to improve the quality of OrientDB enterprise agent and good bug reports are welcome! But our capacity is limited, so we cannot handle questions or consultation requests and we cannot afford to ask for required details. So we reserve the right to close or to not process insufficient bug reports in favor of those which are very cleanly documented and easy to reproduce. Even though we would like to solve each well-documented issue, there is always the chance that it won't happen - remember: OrientDB enterprise agent is Open Source and comes without warranty.

Bug report analysis support is very welcome! (e.g. pre-analysis or proposing solutions)


## Contribute Code

You are welcome to contribute code to OrientDB enterprise agent in order to fix bugs or to implement new features.

There are three important things to know:

1.  You must be aware of the Apache License (which describes contributions) and **agree to the Developer Certificate of Origin**. This is common practice in all major Open Source projects. To make this process as simple as possible, we are using *[CLA assistant](https://cla-assistant.io/)*. CLA assistant is an open source tool that integrates with GitHub very well and enables a one-click-experience for accepting the DCO. See the respective section below for details.
2.  There are **several requirements regarding code style, quality, and product standards** which need to be met (we also have to follow them). The respective section below gives more details on the coding guidelines.
3.  **Not all proposed contributions can be accepted**. Some features may e.g. just fit a third-party add-on better. The code must fit the overall direction of OrientDB enterprise anget and really improve it, so there should be some "bang for the byte". 


### Developer Certificate of Origin (DCO)

Due to legal reasons, contributors will be asked to accept a DCO before they submit the first pull request to this project. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).  
This happens in an automated fashion during the submission process: the CLA assistant tool will add a comment to the pull request. Click it to check the DCO, then accept it on the following screen. CLA assistant will save this decision for upcoming contributions.

This DCO replaces the previously used CLA ("Contributor License Agreement") as well as the "Corporate Contributor License Agreement" with new terms which are well-known standards and hence easier to approve by legal departments. Contributors who had already accepted the CLA in the past may be asked once to accept the new DCO.


