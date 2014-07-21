rm -rf dist
mkdir dist
cp -r *.html dist
cp -r *.png dist
cp -r images dist
cd dist
sed -i "" "s|views/server/asciidoc/||g" *.html
sed -i "" "s|#/dashboard/help|introduction.html|g" *.html
sed -i "" "s|#/dashboard/gettstart|gettingstarted.html|g" *.html
sed -i "" "s|#/help/servermanagement|servermanagement.html|g" *.html
sed -i "" "s|#/help/clustermgmt|clustermgmt.html|g" *.html
sed -i "" "s|#/help/queryprofiler|queryprofiler.html|g" *.html
sed -i "" "s|#/help/notifications|notifications.html|g" *.html
sed -i "" "s|#/help/alerts|alerts.html|g" *.html
sed -i "" "s|#/help/settings|settings.html|g" *.html
sed -i "" "s|#/help/charts|charts.html|g" *.html
cd ..

