%
% Code to visualize profit curve of player 1 in 2 player 2 stage frequency
% fair game, as function of f1, f2
%

function profit_response_curve_GUI

    % Build the GUI
    init_ui;


    % This function sets up the figure and slider.
    % note the CreateFcn and the Callback.
    % The CreatFcn runs the first time while the Callback
    % runs each time you move the slider.
    function init_ui()
        fig1 = figure;
        slider1 = uicontrol('Style', 'slider',...
                            'Min',1.5,'Max',4.9,'Value',1.5,...
                            'Position', [100 10 400 20],...
                            'CreateFcn', @solve_and_plot,...
                            'Callback',  @solve_and_plot); 
    end

%     % These are the parameters for the ODE, I've knackered an
%     % example from the Documentation
%     function param = init_param(Tmax)
%         param.ft    = linspace(0,Tmax,100);             % Generate t for f
%         param.f     = param.ft.^2 - param.ft - 3;   % Generate f(t)
%         param.gt    = linspace(1,6,25);             % Generate t for g
%         param.g     = 3*sin(param.gt-0.25);         % Generate g(t)
%     end
% 
%     % This is a funciton required in the ODE solve 
%     function dydt = myode(t,y,ft,f,gt,g)
%         f = interp1(ft,f,t); % Interpolate the data set (ft,f) at time t
%         g = interp1(gt,g,t); % Interpolate the data set (gt,g) at time t
%         dydt = -f.*y + g; % Evaluate ODE at time t
%     end

    % This funciton it what is run when you first run the 
    % code and each time you move the slider.
    function plot(src,event)

        % Get the current slider value
        alpha = get(src,'Value');
        param_vec = 1.5:.2:5;
        % get greatest param_vec index less than or equal to continuous alpha
        less_than_inds = param_vec<=alpha;
        alpha_ind = numel(less_than_inds);
        %load  data
        fvals_tensor = load('fvals_scurve.mat');
        fvals_tensor = fvals_tensor.fvals_tensor;
%         ps_tensor = load('ps_scurve.mat');
%         ps_tensor = ps_tensor.ps_tensor;
        datatable = load(sprintf('coef_vary_alpha_scurve_%dplayer.mat',2));
        %select data
        fvals = fvals_tensor(:,:,alpha_ind);
%         ps = ps_tensor(:,:,alpha_ind);
        coefs = datatable(alpha_ind,5:10);
        %create figure
        
        [xq,yq] = meshgrid(1:1:20, 1:1:20);
        vq = griddata(fvals(:,7),fvals(:,8),fvals(:,9),xq,yq);
        hold on
        mesh(xq,yq,vq);
        plot3(fvals(:,7),fvals(:,8),fvals(:,9),'o');
        xlabel('p1 freq')
        ylabel('p2 freq')
        h = gca;
        h.XLim = [0 20];
        h.YLim = [0 20];

        
        
        
        % Update parameters
        param = init_param(Tmax);

        Tspan = [1 Tmax];   % Solve from t=1 to t=Tmax
        IC = 1;             % y(t=1) = 1

        % Solve ODE
        [T,Y] = ode45(@(t,y)...
                myode(t,y,param.ft,param.f,param.gt,param.g),...
                Tspan,IC); 

        % Update plot
        plot(T, Y);
        title('Plot of y as a function of time');
        xlabel('Time'); 
        ylabel('Y(t)');

    end

end




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
