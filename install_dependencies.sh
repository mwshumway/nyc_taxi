# Used to download necessary dependencies for the project

if [[ $(uname) == "Darwin" ]]; then # Mac detected, we all have Macs
    echo "Mac Detected"

    python -m pip install --upgrade pip
    python -m pip install --requirement requirements.txt

    
    printf "\nPython dependencies successfully installed!\n"

fi