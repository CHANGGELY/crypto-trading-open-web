# 用途：一键启动本项目的本地网页服务，自动创建虚拟环境并安装依赖
# 说明：脚本在当前目录执行，不修改系统设置；失败会打印中文错误提示。

param(
    [int]$Port = 8000
)

function 写出($msg){ Write-Host $msg -ForegroundColor Cyan }
function 报错($msg){ Write-Host $msg -ForegroundColor Red }

try {
    写出 "检查虚拟环境..."
    if(!(Test-Path ".venv")){
        写出 "创建虚拟环境 .venv"
        python -m venv .venv
    }

    $py = ".\.venv\Scripts\python"
    $pip = ".\.venv\Scripts\pip"
    if(!(Test-Path $py)){ $py = "python" }
    if(!(Test-Path $pip)){ $pip = "pip" }

    写出 "升级 pip 并安装依赖..."
    & $py -X utf8 -m pip install --upgrade pip | Out-Host
    & $pip install pandas | Out-Host

    写出 "启动服务 http://localhost:$Port/"
    $args = @('-X','utf8','-c',"from server import 启动; 启动($Port)")
    Start-Process -FilePath $py -ArgumentList $args -WindowStyle Hidden
    写出 "已启动，稍等 1-2 秒后用浏览器访问 http://localhost:$Port/"
}
catch {
    报错 "启动失败：$($_.Exception.Message)"
    exit 1
}

