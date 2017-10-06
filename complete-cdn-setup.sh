
echo "moving files from cdn relative directory to /var/cdn for nginx static file serving"

sudo cp -a ./cdn/. /var/cdn

echo "changing permission of /var/cdn directory and contents for nginx static file serving"

sudo chown -R www-data:www-data /var/cdn