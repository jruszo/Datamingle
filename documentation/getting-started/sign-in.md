# Sign In

Datamingle uses email/password authentication through django-allauth headless JWT
endpoints. Open your organization Datamingle URL and enter your email and
password.

Public signup is closed. Administrators create local Datamingle user accounts.

## What Happens After Sign-In

After a successful sign-in, the SPA stores JWT access and refresh tokens. Your local Datamingle user stores:

- display name,
- email,
- active state,
- permission groups,
- teams,
- audit identity.

## If Sign-In Fails

Contact a Datamingle administrator with:

- the Datamingle URL,
- the email address you used,
- the approximate time of the failure,
- the displayed error message.
