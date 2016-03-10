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
%Z  = log(xq) + -.5*log(yq) + -.1*xq.^2 + .1*yq.^2 +xq.*yq;
Z = coefs(1).*ones(numel(1:.1:20)) +coefs(2).*xq + coefs(3).*yq +coefs(4).*xq.^2 + coefs(5).*yq.^2 +coefs(6).*xq.*yq;

fig = mesh(xq,yq,Z);
hold on
%plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
plot3(fvals(:,6),fvals(:,7),fvals(:,8),'o')
%plot3(ps(:,6),ps(:,7),ps(:,8),'o')
xlabel('p1 freq')
ylabel('p2 freq')
h = gca;
h.XLim = [0 20];
h.YLim = [0 20];
