1.清理Maven缓存：
mvn dependency:purge-local-repository

2.强制更新依赖：
mvn clean compile -U

3.如果仍有问题，手动清理缓存
# Windows PowerShell
Remove-Item -Recurse -Force "$env:USERPROFILE\.m2\repository\org\apache\hive\"
