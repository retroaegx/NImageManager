const message = document.getElementById("verifyMessage");
const loginLink = document.getElementById("verifyLoginLink");
const token = new URL(location.href).searchParams.get("token") || "";

async function verify(){
  if(!token){ message.textContent = "確認用トークンがありません。"; return; }
  try{
    const response = await fetch("/api/auth/verify-email", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({token}),
      credentials: "include",
    });
    if(!response.ok){
      const data = await response.json().catch(() => ({}));
      message.textContent = response.status === 410 ? "確認用URLの有効期限が切れています。登録画面から再送してください。" : (data?.detail || "確認用URLが無効です。");
      return;
    }
    message.textContent = "メールアドレスを確認しました。ログインできます。";
    message.classList.add("ok");
    loginLink.classList.remove("hidden");
  }catch{
    message.textContent = "接続できません。時間をおいて再度お試しください。";
  }
}
verify();
