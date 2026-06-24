import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'
import 'katex/dist/katex.min.css'

// Initialize i18n
import './i18n/config.js'

// Register service worker for PWA + Web Push support — production only.
// In the vite dev server a service worker intercepts module/HMR/navigation
// requests and can leave them hanging (a request that "never returns" while
// the backend sits idle), and a previously-installed worker keeps controlling
// the page across reloads. In dev, proactively unregister any existing worker
// and drop its caches so dev loads are never served or blocked by it.
if ('serviceWorker' in navigator) {
  if (import.meta.env.DEV) {
    navigator.serviceWorker.getRegistrations()
      .then(registrations => registrations.forEach(registration => registration.unregister()))
      .catch(() => { /* best effort */ });
    if (window.caches) {
      caches.keys().then(keys => keys.forEach(key => caches.delete(key))).catch(() => { /* best effort */ });
    }
  } else {
    navigator.serviceWorker.register('/sw.js').catch(err => {
      console.warn('Service worker registration failed:', err);
    });
  }
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
