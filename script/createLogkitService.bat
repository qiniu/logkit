@echo off
if /i "%1"=="stop" (
    call:stop
    exit
)
SC QUERY logkit > NUL
IF ERRORLEVEL 1060 GOTO NOTEXIST
GOTO EXIST

:start
for /f "skip=3 tokens=4" %%i in ('sc query logkit') do (
	if /i "%%i"=="STOPPED" (
		sc start logkit > nul
		echo starting logkit ...
		ping -n 5 127.0.0.1 > nul

		for /f "skip=3 tokens=4" %%j in ('sc query logkit') do (
			if /i "%%j"=="STOPPED" (
				echo logkit start fail
			) else (
				echo logkit is %%j
			)
			GOTO:EOF
		)

	) else (
		echo logkit is %%i
	)
	GOTO:EOF
)
GOTO:EOF

:restart
call:stop
call:start
GOTO:EOF

:stop
for /f "skip=3 tokens=4" %%s in ('sc query logkit') do (
	if /i not "%%s"=="STOPPED" (
		sc stop logkit > nul
		echo stoping logkit ...
		ping -n 5 127.0.0.1 > nul
		for /f "skip=3 tokens=4" %%i in ('sc query logkit') do (
			if /i "%%i"=="STOPPED" (
				echo logkit stopped
			) else (
				echo logkit is in %%i after 5 seconds
				echo wait for 5 seconds agin
				ping -n 5 127.0.0.1 > nul
				for /f "skip=3 tokens=4" %%j in ('sc query logkit') do (
					if /i not "%%j"=="STOPPED" (
						echo logkit is in %%i After 10 seconds
						exit
					)
					GOTO:EOF
				)
			)
			GOTO:EOF

		)
	) else (
		echo logkit is %%s
	)
	GOTO:EOF
)
GOTO:EOF

:EXIST
for /f "skip=3 tokens=4" %%i in ('sc query logkit') do (
	if /i "%%i"=="STOPPED" (
		echo logkit is STOPPED
		call:start
		GOTO:EOF
	) else (
		if /i "%%i"=="RUNNING" (
		    if /i "%1"=="restart" (
			    call:restart
			    GOTO:EOF
		    )
		)
		echo logkit is %%i
		GOTO:EOF
	)
)
GOTO:EOF

:NOTEXIST
echo install logkit service
del installLogkit.log
nssm install logkit C:\logkit\package\logkit.exe -f C:\logkit\package\logkit.conf >> installLogkit.log 2>>&1
nssm set logkit AppStdout C:\logkit\package\run\logkit.log >> installLogkit.log 2>>&1
nssm set logkit AppStderr C:\logkit\package\run\logkit.log >> installLogkit.log 2>>&1
call:start
GOTO:EOF