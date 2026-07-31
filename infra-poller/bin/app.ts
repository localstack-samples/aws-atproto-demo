#!/usr/bin/env node
import { App } from "aws-cdk-lib";
import { AtprotoPollerStack } from "../lib/atproto-poller-stack";

const app = new App();
new AtprotoPollerStack(app, "AtprotoFirehoseDemoPollerStack");
