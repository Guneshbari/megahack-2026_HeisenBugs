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
import { hasConfiguredApiBearerToken, syncApiSessionAuth } from '../lib/api';

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

function mapFirebaseUser(firebaseUser: FirebaseUser): User {
  return {
    name: firebaseUser.displayName || firebaseUser.email?.split('@')[0] || 'User',
    email: firebaseUser.email || '',
    uid: firebaseUser.uid,
  };
}

interface AuthProviderProps {
  readonly children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (firebaseUser) => {
      syncApiSessionAuth(!!firebaseUser);

      if (firebaseUser) {
        try {
          const profileDoc = await getDoc(doc(db, 'users', firebaseUser.uid));
          if (profileDoc.exists()) {
            const profileData = profileDoc.data();
            setUser({
              name: profileData.name || firebaseUser.displayName || 'User',
              email: firebaseUser.email || '',
              uid: firebaseUser.uid,
            });
          } else {
            setUser(mapFirebaseUser(firebaseUser));
          }
        } catch (error) {
          console.warn('SentinelCore auth profile fallback:', error);
          setUser(mapFirebaseUser(firebaseUser));
        }
      } else {
        setUser(null);
      }

      setIsLoading(false);
    });

    return () => unsubscribe();
  }, []);

  useEffect(() => {
    if (user && !hasConfiguredApiBearerToken) {
      console.warn(
        'SentinelCore frontend login is active, but VITE_SENTINEL_API_BEARER_TOKEN is not configured. Protected backend deployments may reject API requests.',
      );
    }
  }, [user]);

  const login = useCallback(async (email: string, password: string) => {
    await signInWithEmailAndPassword(auth, email, password);
  }, []);

  const signup = useCallback(async (name: string, email: string, password: string) => {
    const credential = await createUserWithEmailAndPassword(auth, email, password);

    await updateProfile(credential.user, { displayName: name });

    await setDoc(doc(db, 'users', credential.user.uid), {
      name,
      email,
      createdAt: serverTimestamp(),
      role: 'operator',
    });

    setUser({
      name,
      email,
      uid: credential.user.uid,
    });
    syncApiSessionAuth(true);
  }, []);

  const logout = useCallback(async () => {
    await signOut(auth);
    setUser(null);
    syncApiSessionAuth(false);
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

export function useAuth(): AuthState {
  const contextValue = useContext(AuthContext);
  if (!contextValue) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return contextValue;
}
