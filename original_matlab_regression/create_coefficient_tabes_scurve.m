%num players
n=2;
%directory
datadir = 'C:/users/d29905P/Documents/airline_competition_paper/';
%profit function
%airline_profit_func = @profit_nplayer_scheddelay;
airline_profit_func =@profit_nplayer_scurve;
%number of regression coefficients
if (n ==1)
    num_coefs = 3;
else
    num_coefs = 1+2*n+nchoosek(n,2);
end

%define defaults
alpha_def = 1.29;
beta_def = -.0045; %as per original model
N_def=0;
S_def = 10000;

%vary alpha
param_vec = 1.5:.2:5;
ps_tensor = zeros(20^n,5+2*n,numel(param_vec));
fvals_tensor = zeros(20^n,5+2*n,numel(param_vec));
datatable_alpha = zeros(numel(param_vec),num_coefs + 6);
tic
for i =1:numel(param_vec);
    alpha = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer_scurve(airline_profit_func,n,alpha,beta_def,S_def,N_def);
    display(sprintf('with alpha=%f, rquared: %f, rsquared adj: %f',alpha,r2,r2adj))
    display(coefs)
    datatable_alpha(i,:) = [alpha,beta_def,S_def,N_def,coefs',r2,r2adj];
    ps_tensor(:,:,i)=ps;
    fvals_tensor(:,:,i)=fvals;
    toc
end
save('fvals_scurve.mat','fvals_tensor')
save('ps_scurve.mat','ps_tensor')
save(sprintf('coef_vary_alpha_scurve_%dplayer.mat',n),'datatable_a')
dlmwrite(sprintf('coef_vary_alpha_scurve_%dplayer.csv',n),datatable_a,'delimiter',',','precision','%.4f')

a = 5;
x=0:.1:20;
y=10;
MS = exp(a*log(x))./(exp(a*log(x))+exp(a*log(y)));
plot(x,MS)


% alpha = 6;
% [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer_scurve(airline_profit_func,n,alpha,beta_def,S_def,N_def);
% display(sprintf('with alpha=%f, rquared: %f, rsquared adj: %f',alpha,r2,r2adj))
% display(coefs)

param_vec = [6 7 8 9 10 15 20 25 30 40 50 100 200 500 1000 10000];
ps_tensor = zeros(20^n,5+2*n,numel(param_vec));
fvals_tensor = zeros(20^n,5+2*n,numel(param_vec));
datatable_alpha = zeros(numel(param_vec),num_coefs + 6);
tic
for i =1:numel(param_vec);
    alpha = param_vec(i);
    [coefs, r2, r2adj, mdl,ps,fvals] = second_stage_nplayer_scurve(airline_profit_func,n,alpha,beta_def,S_def,N_def);
    display(sprintf('with alpha=%f, rquared: %f, rsquared adj: %f',alpha,r2,r2adj))
    display(coefs)
    datatable_a(i,:) = [alpha,beta_def,S_def,N_def,coefs',r2,r2adj];
    ps_tensor(:,:,i)=ps;
    fvals_tensor(:,:,i)=fvals;
    toc
end
save('fvals_scurve_xtreme.mat','fvals_tensor')
save('ps_scurve_xtreme.mat','ps_tensor')
save(sprintf('coef_vary_alpha_scurve_%dplayer_xtreme.mat',n),'datatable_a')
dlmwrite(sprintf('coef_vary_alpha_scurve_%dplayer_xtreme.csv',n),datatable_a,'delimiter',',','precision','%.4f')

f = load('fvals_scurve.mat')
fx = load('fvals_scurve_xtreme.mat')
ps = load('ps_scurve.mat')
psx = load('ps_scurve_xtreme.mat')
coef_mat = load(sprintf('coef_vary_alpha_scurve_%dplayer.mat',n))
coef_matx = load(sprintf('coef_vary_alpha_scurve_%dplayer_xtreme.mat',n))

param_vec = [6 7 8 9 10 15 20 25 30 40 50 100 200 500 1000 10000]
for i=1:numel(param_vec)
    fvals = fx.fvals_tensor(:,:,i);
    coefs = coef_matx.datatable_a(i,5:10);
    
    %plot against quadratic approximation
    [xq,yq] = meshgrid(1:.1:20, 1:.1:20);
    %Z  = log(xq) + -.5*log(yq) + -.1*xq.^2 + .1*yq.^2 +xq.*yq;
    Z = coefs(1).*ones(numel(1:.1:20)) +coefs(2).*xq + coefs(3).*yq +coefs(4).*xq.^2 + coefs(5).*yq.^2 +coefs(6).*xq.*yq;

    fig = mesh(xq,yq,Z);
    hold on
    %plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
    plot3(fvals(:,6),fvals(:,7),fvals(:,8),'o');
    %plot3(ps(:,6),ps(:,7),ps(:,8),'o')
    xlabel('p1 freq');
    ylabel('p2 freq');
    title(sprintf('alpha = %d, %f | %f | %f  | %f | %f  | %f ',param_vec(i), coefs));
    h = gca;
    h.XLim = [0 20];
    h.YLim = [0 20];
    saveas(fig,sprintf('alpha_%d',param_vec(i)))
    hold off
end


%generate model
k=ps.ps_tensor(:,:,1);
X=[log(k(:,6)),log(k(:,7)),log(k(:,6)+k(:,7))];
%profits
Y =k(:,8);
%fit model
mdl = fitlm(X,Y);
%coefficients
coefs = mdl.Coefficients.Estimate;
%Rsquared
r2 = mdl.Rsquared.Ordinary;
r2adj = mdl.Rsquared.Adjusted;


%plot against  approximation
[xq,yq] = meshgrid(1:.1:20, 1:.1:20);
%Z  = log(xq) + -.5*log(yq) + -.1*xq.^2 + .1*yq.^2 +xq.*yq;
Z = coefs(1).*ones(numel(1:.1:20)) +coefs(2).*log(xq) + coefs(3).*log(yq) + coefs(4).*log(xq+ yq) ;

fig = mesh(xq,yq,Z);
[xq,yq] = meshgrid(1:.1:20, 1:.1:20);
fig = mesh(xq,yq,Z);
hold on
%plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
plot3(k(:,6),k(:,7),k(:,8),'o');
figure
plotResiduals(mdl)




%so...log model better fit...but doesnt capture concavity at high values of
%f2
%generate model
k=fvals_tensor(:,:,6);
%%X=[log(k(:,6)),log(k(:,7)),log(k(:,6)+k(:,7)), k(:,6).*log(k(:,7)),k(:,7).*log(k(:,6))];
X=[log(k(:,6)),log(k(:,7)),log(k(:,6)+k(:,7)), k(:,6).*log(k(:,7))];
%profits
Y =k(:,8);
%fit model
mdl = fitlm(X,Y);
%coefficients
coefs = mdl.Coefficients.Estimate;
%Rsquared
r2 = mdl.Rsquared.Ordinary;
r2adj = mdl.Rsquared.Adjusted;


%plot against  approximation
[xq,yq] = meshgrid(1:.1:20, 1:.1:20);
%Z  = log(xq) + -.5*log(yq) + -.1*xq.^2 + .1*yq.^2 +xq.*yq;
%%Z = coefs(1).*ones(numel(1:.1:20)) +coefs(2).*log(xq) + coefs(3).*log(yq) + coefs(4).*log(xq+ yq) +coefs(5).*xq.*log(yq)+coefs(6).*yq.*log(xq);
Z = coefs(1).*ones(numel(1:.1:20)) +coefs(2).*log(xq) + coefs(3).*log(yq) + coefs(4).*log(xq+ yq) +coefs(5).*xq.*log( yq);
%no..ned something that  makes concave contigent on f2 (reduces high f1,low
%f1 if f2 is high...maybe look at derived formula again
fig = mesh(xq,yq,Z);
[xq,yq] = meshgrid(1:.1:20, 1:.1:20);
fig = mesh(xq,yq,Z);
hold on
%plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
plot3(k(:,6),k(:,7),k(:,8),'o');
figure
%plot residuals (should do this in 3d huuuuh
plotResiduals(mdl)


%test
 a=k(k(:,7)==20,:);
 figure
 plot(a(:,6),a(:,8))
 
