import { initializeApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: "AIzaSyDY844QFgCtn1l4rufgoWXBySW-xYrKDAg",
  authDomain: "sentinelcore-99210.firebaseapp.com",
  projectId: "sentinelcore-99210",
  storageBucket: "sentinelcore-99210.firebasestorage.app",
  messagingSenderId: "497997127446",
  appId: "1:497997127446:web:3f2ae875ac9ec070c0fa37",
};

const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export const db = getFirestore(app);
