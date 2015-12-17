%2 player visualization
[xq,yq] = meshgrid(1:1:20, 1:1:20);
vq = griddata(fvals(:,7),fvals(:,8),fvals(:,9),xq,yq);
figure
mesh(xq,yq,vq);
hold on
plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
xlabel('p1 freq')
ylabel('p2 freq')
h = gca;
h.XLim = [0 20];
h.YLim = [0 20];


%plot against quadratic approximation
[xq,yq] = meshgrid(1:.1:20, 1:.1:20);
Z = coefs(1).*ones(numel(1:.1:20)) +coefs(2).*xq + coefs(3).*yq +coefs(4).*xq.^2 + coefs(5).*yq.^2 +coefs(6).*xq.*yq;
figure
mesh(xq,yq,Z);
hold on
%plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
plot3(fs(:,1),fs(:,2),fs(:,3),'o')
xlabel('p1 freq')
ylabel('p2 freq')
h = gca;
h.XLim = [0 20];
h.YLim = [0 20];
