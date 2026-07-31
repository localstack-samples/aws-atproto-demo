#!/usr/bin/env node
import { App } from "aws-cdk-lib";
import { AtprotoFargateStack } from "../lib/atproto-fargate-stack";

const app = new App();
new AtprotoFargateStack(app, "AtprotoFirehoseDemoFargateStack");
