import { bindUserMenu } from "./lib/userMenu.js";
import { loadCurrentUser, isAdminRole } from "./lib/session.js";

const API = {
  me: "/api/me",
  logout: "/api/auth/logout",
  passwordLink: "/api/auth/password_link",
};

loadCurrentUser({
  endpoint: API.me,
  onLoaded: (me) => {
    const showAdmin = isAdminRole(String(me?.role || "user"));
    bindUserMenu({
      logoutEndpoint: API.logout,
      passwordLinkEndpoint: API.passwordLink,
      showAdmin,
      showMaintenance: showAdmin,
    });
  },
}).catch(() => {
  location.replace("/login.html");
});
