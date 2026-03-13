import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from 'react';
import {
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  signOut,
  onAuthStateChanged,
  updateProfile,
  type User as FirebaseUser,
} from 'firebase/auth';
import { doc, setDoc, getDoc, serverTimestamp } from 'firebase/firestore';
import { auth, db } from '../lib/firebase';

// ── Types ───────────────────────────────────────────────
interface User {
  name: string;
  email: string;
  uid: string;
}

interface AuthState {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (email: string, password: string) => Promise<void>;
  signup: (name: string, email: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthState | null>(null);

// ── Helper: map Firebase user to our User type ───────────
function mapFirebaseUser(fbUser: FirebaseUser): User {
  return {
    name: fbUser.displayName || fbUser.email?.split('@')[0] || 'User',
    email: fbUser.email || '',
    uid: fbUser.uid,
  };
}

// ── Helper: map Firebase error codes to friendly messages ─
export function getFirebaseErrorMessage(code: string): string {
  switch (code) {
    case 'auth/email-already-in-use':
      return 'This email is already registered. Try signing in instead.';
    case 'auth/invalid-email':
      return 'Please enter a valid email address.';
    case 'auth/invalid-credential':
      return 'Invalid email or password. Please try again.';
    case 'auth/user-not-found':
      return 'No account found with this email. Please sign up.';
    case 'auth/wrong-password':
      return 'Incorrect password. Please try again.';
    case 'auth/weak-password':
      return 'Password must be at least 6 characters.';
    case 'auth/too-many-requests':
      return 'Too many attempts. Please try again later.';
    case 'auth/network-request-failed':
      return 'Network error. Please check your connection.';
    default:
      return 'An unexpected error occurred. Please try again.';
  }
}

// ── Provider ─────────────────────────────────────────────
interface AuthProviderProps {
  readonly children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  // Listen for Firebase auth state changes
  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (fbUser) => {
      if (fbUser) {
        // Try to get extra profile data from Firestore
        try {
          const profileDoc = await getDoc(doc(db, 'users', fbUser.uid));
          if (profileDoc.exists()) {
            const data = profileDoc.data();
            setUser({
              name: data.name || fbUser.displayName || 'User',
              email: fbUser.email || '',
              uid: fbUser.uid,
            });
          } else {
            setUser(mapFirebaseUser(fbUser));
          }
        } catch {
          // Fallback if Firestore read fails
          setUser(mapFirebaseUser(fbUser));
        }
      } else {
        setUser(null);
      }
      setIsLoading(false);
    });

    return () => unsubscribe();
  }, []);

  const login = useCallback(async (email: string, password: string) => {
    await signInWithEmailAndPassword(auth, email, password);
    // onAuthStateChanged will update the user state
  }, []);

  const signup = useCallback(async (name: string, email: string, password: string) => {
    const credential = await createUserWithEmailAndPassword(auth, email, password);

    // Set display name on Firebase Auth profile
    await updateProfile(credential.user, { displayName: name });

    // Store user profile in Firestore
    await setDoc(doc(db, 'users', credential.user.uid), {
      name,
      email,
      createdAt: serverTimestamp(),
      role: 'operator',
    });

    // Update local state immediately (don't wait for onAuthStateChanged)
    setUser({
      name,
      email,
      uid: credential.user.uid,
    });
  }, []);

  const logout = useCallback(async () => {
    await signOut(auth);
    setUser(null);
  }, []);

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        isLoading,
        login,
        signup,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

// ── Hook ─────────────────────────────────────────────────
export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
