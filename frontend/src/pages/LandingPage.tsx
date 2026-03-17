import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { Shield, ArrowRight, Loader2, Eye, EyeOff, Activity, Server, Zap, Lock } from 'lucide-react';
import { useAuth, getFirebaseErrorMessage } from '../context/AuthContext';
import NetworkAnimation from '../components/three/NetworkAnimation';

export default function LandingPage() {
  const [mode, setMode] = useState<'login' | 'signup'>('login');
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState('');

  const { login, signup } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError('');

    if (!email || !password) {
      setError('Please fill in all required fields.');
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      setError('Please enter a valid email address.');
      return;
    }

    if (mode === 'signup') {
      if (!name) {
        setError('Please enter your name.');
        return;
      }
      if (password !== confirmPassword) {
        setError('Passwords do not match.');
        return;
      }
      if (password.length < 8) {
        setError('Password must be at least 8 characters long.');
        return;
      }
      if (!/(?=.*[a-z])/.test(password)) {
        setError('Password must contain at least one lowercase letter.');
        return;
      }
      if (!/(?=.*[A-Z])/.test(password)) {
        setError('Password must contain at least one uppercase letter.');
        return;
      }
      if (!/(?=.*\d)/.test(password)) {
        setError('Password must contain at least one number.');
        return;
      }
      if (!/(?=.*[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?])/.test(password)) {
        setError('Password must contain at least one special character.');
        return;
      }
    }

    setIsSubmitting(true);
    try {
      if (mode === 'login') {
        await login(email, password);
      } else {
        await signup(name, email, password);
      }
      navigate('/');
    } catch (err: unknown) {
      const firebaseError = err as { code?: string };
      if (firebaseError.code) {
        setError(getFirebaseErrorMessage(firebaseError.code));
      } else {
        setError('Authentication failed. Please try again.');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const switchMode = () => {
    setMode(mode === 'login' ? 'signup' : 'login');
    setError('');
    setPassword('');
    setConfirmPassword('');
  };

  const features = [
    { icon: Activity, label: 'Real-Time Monitoring', desc: 'Track system health across your distributed infrastructure' },
    { icon: Server, label: 'Multi-System View', desc: 'Unified dashboard for all your Windows telemetry endpoints' },
    { icon: Zap, label: 'Fault Detection', desc: 'AI-powered anomaly detection with instant severity classification' },
    { icon: Lock, label: 'SOC Console', desc: 'Security operations center with advanced threat analytics' },
  ];

  return (
    <div className="landing-page">
      {/* 3D Background */}
      <NetworkAnimation />

      {/* Content overlay */}
      <div className="landing-content">
        {/* Left — Hero / Branding */}
        <div className="landing-hero">
          <div className="landing-hero-inner">
            {/* Logo */}
            <div className="landing-logo">
              <div className="landing-logo-icon">
                <Shield className="w-8 h-8 text-signal-primary" />
              </div>
              <div>
                <h1 className="landing-title">SentinelCore</h1>
                <p className="landing-subtitle">Distributed Monitoring Platform</p>
              </div>
            </div>

            {/* Tagline */}
            <h2 className="landing-tagline">
              Intelligent Telemetry
              <br />
              <span className="landing-tagline-accent">for Distributed Systems</span>
            </h2>

            <p className="landing-description">
              Monitor, analyze, and secure your distributed Windows infrastructure 
              in real time. SentinelCore aggregates telemetry data from across your 
              network, detects anomalies with AI-powered analysis, and provides 
              actionable insights through an intuitive SOC dashboard.
            </p>

            {/* Feature grid */}
            <div className="landing-features">
              {features.map(({ icon: Icon, label, desc }) => (
                <div key={label} className="landing-feature-card">
                  <div className="landing-feature-icon">
                    <Icon className="w-5 h-5" />
                  </div>
                  <div>
                    <h3 className="landing-feature-label">{label}</h3>
                    <p className="landing-feature-desc">{desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Right — Auth Form */}
        <div className="landing-form-section">
          <div className="landing-form-card">
            {/* Form header */}
            <div className="landing-form-header">
              <h2 className="landing-form-title">
                {mode === 'login' ? 'Welcome Back' : 'Create Account'}
              </h2>
              <p className="landing-form-subtitle">
                {mode === 'login'
                  ? 'Sign in to access your monitoring dashboard'
                  : 'Get started with SentinelCore monitoring'}
              </p>
            </div>

            {/* Error */}
            {error && (
              <div className="landing-error">
                {error}
              </div>
            )}

            {/* Form */}
            <form onSubmit={handleSubmit} className="landing-form">
              {mode === 'signup' && (
                <div className="landing-field">
                  <label htmlFor="name" className="landing-label">Full Name</label>
                  <input
                    id="name"
                    type="text"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder="Enter your name"
                    className="landing-input"
                    autoComplete="name"
                  />
                </div>
              )}

              <div className="landing-field">
                <label htmlFor="email" className="landing-label">Email</label>
                <input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@company.com"
                  className="landing-input"
                  autoComplete="email"
                />
              </div>

              <div className="landing-field">
                <label htmlFor="password" className="landing-label">Password</label>
                <div className="landing-password-wrap">
                  <input
                    id="password"
                    type={showPassword ? 'text' : 'password'}
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    className="landing-input"
                    autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(!showPassword)}
                    className="landing-password-toggle"
                  >
                    {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </button>
                </div>
              </div>

              {mode === 'signup' && (
                <div className="landing-field">
                  <label htmlFor="confirmPassword" className="landing-label">Confirm Password</label>
                  <input
                    id="confirmPassword"
                    type="password"
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    placeholder="••••••••"
                    className="landing-input"
                    autoComplete="new-password"
                  />
                </div>
              )}

              <button
                type="submit"
                disabled={isSubmitting}
                className="landing-submit"
              >
                {isSubmitting ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <>
                    {mode === 'login' ? 'Sign In' : 'Create Account'}
                    <ArrowRight className="w-4 h-4" />
                  </>
                )}
              </button>
            </form>

            {/* Switch mode */}
            <div className="landing-switch">
              <span className="landing-switch-text">
                {mode === 'login' ? "Don't have an account?" : 'Already have an account?'}
              </span>
              <button onClick={switchMode} className="landing-switch-btn">
                {mode === 'login' ? 'Sign Up' : 'Sign In'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
