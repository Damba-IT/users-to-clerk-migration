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

const userSchema = z.object({
  _id: z.object({
    $oid: z.string(),
  }),
  email: z.string().email(),
  first_name: z.string().optional(),
  last_name: z.string().optional(),
  phone: z.string().optional(),
  password: z.string().optional(),
  organization_id: z.string().optional(),
  locale: z.string().optional(),
  mobile_organization_id: z.string().optional(),
  role_id: z.string().optional(),
  super_admin: z.boolean().optional(),
  // ... other fields as necessary
});

type User = z.infer<typeof userSchema>;

const createUser = async (userData: User) => {
  // function formatPhoneNumberToE164(phone, defaultCountryCode = "46") {
  //   if (!phone.startsWith("+")) {
  //     return `${defaultCountryCode}${phone}`;
  //   }
  //   return phone as string;
  // }

  const externalId = userData._id.$oid;
  //const formattedPhone = formatPhoneNumberToE164(userData.phone);

  const privateMetaData = {
    mongoId: userData._id,
    organizationId: userData.organization_id,
    mobileOrganizationId: userData.mobile_organization_id,
  };
  const publicMetadata = {
    roleId: userData.role_id ?? "5be5659706b068f4f1f2dd53",
    locale: userData.locale ?? "sv",
    superAdmin: userData.super_admin ?? false,
  };

  return userData.password &&
    userData.password.length >= 8 &&
    userData.password.startsWith("123")
    ? await clerkClient.users.createUser({
        externalId: externalId,
        emailAddress: [userData.email],
        firstName: userData.first_name,
        lastName: userData.last_name,
        //phoneNumber: [formattedPhone], //not working for some reason
        privateMetadata: privateMetaData,
        publicMetadata: publicMetadata,
        password: userData.password,
      })
    : await clerkClient.users.createUser({
        externalId: externalId,
        emailAddress: [userData.email],
        firstName: userData.first_name,
        lastName: userData.last_name,
        //phoneNumber: [formattedPhone], //not working for some reason
        privateMetadata: privateMetaData,
        publicMetadata: publicMetadata,
        skipPasswordRequirement: true,
      });
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

async function processUserToClerk(userData: User, spinner: Ora) {
  const txt = spinner.text;
  try {
    const parsedUserData = userSchema.safeParse(userData);
    if (!parsedUserData.success) {
      throw parsedUserData.error;
    }
    await createUser(parsedUserData.data);

    migrated++;
  } catch (error) {
    if (error.status === 422) {
      appendLog({ userId: userData._id, ...error });
      alreadyExists++;
      return;
    }

    // Keep cooldown in case rate limit is reached as a fallback if the thread blocking fails
    if (error.status === 429) {
      spinner.text = `${txt} - rate limit reached, waiting for ${RETRY_DELAY} ms`;
      await rateLimitCooldown();
      spinner.text = txt;
      return processUserToClerk(userData, spinner);
    }

    appendLog({ userId: userData._id, ...error });
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

  const inputFileName = process.argv[2] ?? "users.json";

  console.log(`Fetching users from ${inputFileName}`);

  const parsedUserData: any[] = JSON.parse(
    fs.readFileSync(inputFileName, "utf-8")
  );
  const offsetUsers = parsedUserData.slice(OFFSET);
  console.log(
    `users.json found and parsed, attempting migration with an offset of ${OFFSET}`
  );

  let i = 0;
  const spinner = ora(`Migrating users`).start();

  for (const userData of offsetUsers) {
    spinner.text = `Migrating user ${i}/${offsetUsers.length}, cooldown`;
    await cooldown();
    i++;
    spinner.text = `Migrating user ${i}/${offsetUsers.length}`;
    await processUserToClerk(userData, spinner);
  }

  spinner.succeed(`Migration complete`);
  return;
}

main().then(() => {
  console.log(`${migrated} users migrated`);
  console.log(`${alreadyExists} users failed to upload`);
});
