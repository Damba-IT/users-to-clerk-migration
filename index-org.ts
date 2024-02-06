import { config } from "dotenv";
config();

import * as fs from "fs";
import * as z from "zod";
import clerkClient from "@clerk/clerk-sdk-node";
import ora, { Ora } from "ora";

const SECRET_KEY = process.env.CLERK_SECRET_KEY;
const DELAY = parseInt(process.env.DELAY_MS ?? `1000`);
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY_MS ?? `10000`);
const IMPORT_TO_DEV = process.env.IMPORT_TO_DEV_INSTANCE ?? "false";
const OFFSET = parseInt(process.env.OFFSET ?? `0`);

if (!SECRET_KEY) {
  throw new Error(
    "CLERK_SECRET_KEY is required. Please copy .env.example to .env and add your key."
  );
}

if (SECRET_KEY.split("_")[1] !== "live" && IMPORT_TO_DEV === "false") {
  throw new Error(
    "The Clerk Secret Key provided is for a development instance. Development instances are limited to 500 users and do not share their userbase with production instances. If you want to import users to your development instance, please set 'IMPORT_TO_DEV_INSTANCE' in your .env to 'true'."
  );
}

const organizationSchema = z.object({
  _id: z.object({
    $oid: z.string(),
  }),
  organization_name: z.string(),
  email: z.string(),
});

type Organization = z.infer<typeof organizationSchema>;

const createOrganization = async (organizationData: Organization) => {
  const externalId = organizationData._id.$oid;
  //   const slug = organizationData.organization_name
  //     .replace(/\s+/g, "-")
  //     .toLowerCase();
  const privateMetaData = {
    mongoId: organizationData._id,
    external_id: externalId,
  };
  let createdBy = await getUserByEmail(organizationData.email);
  return await clerkClient.organizations.createOrganization({
    name: organizationData.organization_name,
    //slug: slug,
    createdBy: createdBy,
    privateMetadata: privateMetaData,
  });
};

const getUserByEmail = async (email: string) => {
  try {
    const users = await clerkClient.users.getUserList({
      emailAddress: [email],
    });
    const user = users.length > 0 ? users[0] : null;
    let userId = "";
    if (user) {
      userId = user.id;
    }
    return userId;
  } catch (error) {
    console.error("Error fetching user:", error);
    return "";
  }
};

const now = new Date().toISOString().split(".")[0]; // YYYY-MM-DDTHH:mm:ss
function appendLog(payload: any) {
  fs.appendFileSync(
    `./migration-log-${now}.json`,
    `\n${JSON.stringify(payload, null, 2)}`
  );
}

let migrated = 0;
let alreadyExists = 0;

async function processOrganizationToClerk(
  organizationData: Organization,
  spinner: Ora
) {
  const txt = spinner.text;
  try {
    const parsedOrganizationData =
      organizationSchema.safeParse(organizationData);
    if (!parsedOrganizationData.success) {
      throw parsedOrganizationData.error;
    }
    await createOrganization(parsedOrganizationData.data);

    migrated++;
  } catch (error) {
    if (error.status === 422) {
      appendLog({ organizationId: organizationData._id, ...error });
      alreadyExists++;
      return;
    }

    // Keep cooldown in case rate limit is reached as a fallback if the thread blocking fails
    if (error.status === 429) {
      spinner.text = `${txt} - rate limit reached, waiting for ${RETRY_DELAY} ms`;
      await rateLimitCooldown();
      spinner.text = txt;
      return processOrganizationToClerk(organizationData, spinner);
    }

    appendLog({ userId: organizationData._id, ...error });
  }
}

async function cooldown() {
  await new Promise((r) => setTimeout(r, DELAY));
}

async function rateLimitCooldown() {
  await new Promise((r) => setTimeout(r, RETRY_DELAY));
}

async function main() {
  console.log(`Clerk User Migration Utility`);

  const inputFileName = process.argv[2] ?? "organizations.json";

  console.log(`Fetching organizations from ${inputFileName}`);

  const parsedOrganizationData: any[] = JSON.parse(
    fs.readFileSync(inputFileName, "utf-8")
  );
  const offsetOrganizations = parsedOrganizationData.slice(OFFSET);
  console.log(
    `organizations.json found and parsed, attempting migration with an offset of ${OFFSET}`
  );

  let i = 0;
  const spinner = ora(`Migrating users`).start();

  for (const organizationData of offsetOrganizations) {
    spinner.text = `Migrating user ${i}/${offsetOrganizations.length}, cooldown`;
    await cooldown();
    i++;
    spinner.text = `Migrating user ${i}/${offsetOrganizations.length}`;
    await processOrganizationToClerk(organizationData, spinner);
  }

  spinner.succeed(`Migration complete`);
}

main().then(() => {
  console.log(`${migrated} organizations migrated`);
  console.log(`${alreadyExists} organizations failed to upload`);
});
