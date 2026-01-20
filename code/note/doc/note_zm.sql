notepad 多行编辑 列模式 
      1、alt +鼠标
      2、列模式 快捷键 alt+c
	  3、按住alt+shift ，鼠标点击要结束的地方或者使用箭头
	  
	  
	  
	  
	  
-- 安装scoop
	-- cmd命令行执行如下
# 安装Scoop
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
irm get.scoop.sh | iex

-- 1. 添加必要的软件源（bucket）
# 添加 extras bucket（通常包含桌面应用）
scoop bucket add extras

# 添加 versions bucket（包含软件的不同版本）
scoop bucket add versions



# 安装 Tor Browser
scoop install tor-browser



# 启动 Tor Browser
tor-browser
# 或者
start tor-browser


# 更新 Tor Browser
scoop update tor-browser

# 更新所有软件
scoop update *

# 卸载 Tor Browser
scoop uninstall tor-browser

# 清理缓存
scoop cache rm tor-browser


-- 安装 Winget

-- 检查 Winget 状态
-- 在 PowerShell 或 CMD 中运行：
winget --version

# 从 GitHub 下载并安装 Winget
$url = "https://github.com/microsoft/winget-cli/releases/latest/download/Microsoft.DesktopAppInstaller_8wekyb3d8bbwe.msixbundle"
$installer = "$env:TEMP\winget-latest.msixbundle"
Invoke-WebRequest -Uri $url -OutFile $installer
Add-AppxPackage -Path $installer