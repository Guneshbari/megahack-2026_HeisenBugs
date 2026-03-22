# Firebase Admin SDK Setup Guide

This guide explains how to enable Firebase Authentication token verification in the SentinelCore backend API.

## 1. Obtain a Service Account Key
1. Go to the [Firebase Console](https://console.firebase.google.com/).
2. Select your project.
3. In the top-left navigation menu, go to **Project settings** (the gear icon) > **Service accounts**.
4. Make sure **Firebase Admin SDK** is selected.
5. Click **Generate new private key**, then confirm by clicking **Generate key**.
6. A `.json` file containing your service account credentials will be downloaded. 

## 2. Secure the Key File
1. Move the downloaded JSON file to a secure location on your SentinelCore server (e.g., `/etc/sentinelcore/firebase-key.json` or equivalent protected directory on Windows).
2. Do **not** commit this file to version control. It provides full administrative access to your Firebase project.

## 3. Configure SentinelCore Environment
Update your `.env` file or environment variables to enable Firebase Auth and point the backend to the key:

```env
# Enable token verification security middleware in the API
SENTINEL_FIREBASE_AUTH_ENABLED=true

# Provide the absolute path to your JSON key file
SENTINEL_FIREBASE_SERVICE_ACCOUNT_PATH=C:/secure/path/to/firebase-key.json
```

> **Note on Cloud Deployments:** If you are running SentinelCore on Google Cloud (e.g., Cloud Run or GKE) with attached service accounts, you do not need to provide a JSON key. Just set `SENTINEL_FIREBASE_AUTH_ENABLED=true`, and the application will automatically use Application Default Credentials to authenticate.
