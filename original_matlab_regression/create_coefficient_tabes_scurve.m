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
    datatable_a(i,:) = [alpha,beta_def,S_def,N_def,coefs',r2,r2adj];
    ps_tensor(:,:,i)=ps;
    fvals_tensor(:,:,i)=fvals;
    toc
end
save('fvals_scurve.mat','fvals_tensor')
save('ps_scurve.mat','ps_tensor')
save(sprintf('coef_vary_alpha_scurve_%dplayer.mat',n),'datatable_m')
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
